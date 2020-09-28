#!/usr/bin/env python3
import argparse
import fnmatch
import hashlib
import itertools
import multiprocessing
import os
import queue
import random
import sqlite3
import sys
import threading
import time
from typing import List, Set


class DB:

    def record_file(
        self,
        abs_path: str,
        base_name: str,
        dir_name: str,
        extension: str,
        size: int,
        mtime: float,
        md5_hex: str
    ) -> None:
        self._queue.put_nowait(('replace into files values (?, ?, ?, ?, ?, ?, ?)',
                                (abs_path, base_name, dir_name, extension, size, mtime, md5_hex)))

    def record_directory(
        self,
        abs_path: str,
        file_count: int,
        dir_count: int,
        symlink_count: int,
    ) -> None:
        self._queue.put_nowait(('replace into directories values (?, ?, ?, ?, ?)',
                                (abs_path, file_count, dir_count, symlink_count, time.time())))

    def record_symlink(self, abs_path: str, target: str) -> None:
        self._queue.put_nowait((
            'replace into symlinks values (?, ?)', (abs_path, target)))

    def record_failure(self, abs_path: str, e: Exception) -> None:
        self._queue.put_nowait((
            'replace into failures values(?, ?, ?)', (abs_path,
                                                      time.time(), str(e))
        ))

    def stop(self) -> None:
        self._queue.put("STOP")

    def get_done_dirs(self) -> Set[str]:
        dbh = sqlite3.connect(self._path)
        done_dirs = set(itertools.chain.from_iterable(dbh.execute(
            "select abs_path from directories").fetchall()))
        dbh.close()
        return done_dirs

    def writer_loop(self):
        dbh = sqlite3.connect(self._path)
        while True:
            try:
                if random.random() < 0.007:
                    print("Write queue length: %d" % (self._queue.qsize()))
                op = self._queue.get(timeout=1)
                if op == "STOP":
                    dbh.commit()
                    return
                dbh.execute(*op)
            except queue.Empty:
                dbh.commit()

    def __init__(self, path: str, que: queue.Queue):
        self._path = path
        self._queue = que


class Scanner:

    def loop(self):
        """Grab work from the queue until it has been empty for 60s."""
        try:
            while True:
                abs_path = self._queue.get(timeout=60)
                try:
                    self._scan_dir(abs_path)
                except Exception as e:
                    self._db.record_failure(abs_path, e)
        except queue.Empty:
            print("Thread finished after 60s of empty queue.")

    def enqueue(self, path: str):
        """Add the absolute path to the queue."""
        self._queue.put(path)

    def _scan_dir(self, path: str):
        """Add subdirectories to the queue and process files."""
        done = path in self._done_dirs
        print("%sProcessing %s (%d in queue)" %
              ("Re-" if done else "", path, self._queue.qsize()))
        files = []
        dir_count = 0
        symlink_count = 0
        for item in os.scandir(path):
            name = item.name
            abs_item = os.path.join(path, name)
            if item.is_dir():
                if not self._ignored(name):
                    self.enqueue(abs_item)
                dir_count += 1
            elif not done:
                if item.is_file():
                    files.append(item)
                else:
                    assert item.is_symlink()
                    self._db.record_symlink(abs_item, os.readlink(abs_item))
                    symlink_count += 1
        if not done:
            for item in files:
                self._process_file(item)
            self._db.record_directory(
                path, len(files), dir_count, symlink_count)
    
    def _ignored(self, name: str) -> bool:
        for pattern in self._ignore:
            if fnmatch.fnmatch(name, pattern):
                return True
        return False

    def _process_file(self, entry: os.DirEntry) -> None:
        abs_path = entry.path
        name = entry.name
        if '.' in name:
            extension = name.split('.')[-1]
        else:
            extension = ''
        stat = os.stat(abs_path)
        self._db.record_file(abs_path, name, os.path.dirname(
            abs_path), extension, stat.st_size, stat.st_mtime, md5(abs_path))

    def __init__(self, db: DB, que: queue.Queue, ignore: List[str], done_dirs: Set[str]):
        self._db = db
        self._queue = que
        self._ignore = ignore
        self._done_dirs = done_dirs


def md5(fname: str):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def str_list(input: str) -> List[str]:
    """Parses a list of comma-separated strings into a list of str."""
    return input.split(',')

def main():
    parser = argparse.ArgumentParser(
        description='Picture collection reorganisation tool')
    parser.add_argument('paths', nargs='+', type=str)
    parser.add_argument('--db', type=str, default='reorg.db')
    parser.add_argument('--workers', type=int, default=os.cpu_count() * 2)
    parser.add_argument('--threading', type=bool, default=False)
    parser.add_argument('--queue_length', type=int, default=0)
    parser.add_argument('--writer_queue_length', type=int, default=1000)
    # `.backupdb` is extension used by Time Machine. It uses directory hard
    # links, which confuse rsync. 
    parser.add_argument('--ignore', type=str_list, default="*.backupdb")
    args = parser.parse_args(sys.argv)
    if args.threading:
        que = queue.Queue(maxsize=args.queue_length)
        dque = queue.Queue(maxsize=args.writer_queue_length)
    else:
        que = multiprocessing.Queue(maxsize=args.queue_length)
        dque = multiprocessing.Queue(maxsize=args.queue_length)
    db = DB(args.db, dque)
    scanner = Scanner(db, que, args.ignore, db.get_done_dirs())
    for path in args.paths:
        scanner.enqueue(os.path.abspath(path))
    working_class = threading.Thread if args.threading else multiprocessing.Process
    workers = []
    for _ in range(args.workers):
        worker = working_class(target=scanner.loop)
        worker.start()
        workers.append(worker)
    writer = working_class(target=db.writer_loop)
    writer.start()
    for worker in workers:
        worker.join()
    db.stop()


if __name__ == '__main__':
    main()
