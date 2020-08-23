#!/usr/bin/env python3
import argparse
import hashlib
import multiprocessing
import os
import queue
import sqlite3
import sys
import threading
import time


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
        self._queue.put(('replace into files values (?, ?, ?, ?, ?, ?, ?)',
                         (abs_path, base_name, dir_name, extension, size, mtime, md5_hex)))

    def record_directory(
        self,
        abs_path: str,
        file_count: int,
        dir_count: int,
        symlink_count: int,
    ) -> None:
        self._queue.put(('replace into directories values (?, ?, ?, ?, ?)',
                         (abs_path, file_count, dir_count, symlink_count, time.time())))

    def record_symlink(self, abs_path: str, target: str) -> None:
        self._queue.put((
            'replace into symlinks values (?, ?)', (abs_path, target)))

    def stop(self) -> None:
        self._queue.put("STOP")

    def writer_loop(self):
        dbh = sqlite3.connect(self._path)
        while True:
            try:
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
                self._scan_dir(self._queue.get(timeout=60))
        except queue.Empty:
            print("Thread finished after 60s of empty queue.")

    def enqueue(self, path: str):
        """Add the absolute path to the queue."""
        self._queue.put(path)

    def _scan_dir(self, path: str):
        """Add subdirectories to the queue and process files."""
        print("Processing %s (%d in queue)" % (path, self._queue.qsize()))
        files = []
        dir_count = 0
        symlink_count = 0
        for item in os.scandir(path):
            name = item.name
            abs_item = os.path.join(path, name)
            if item.is_dir():
                self.enqueue(abs_item)
                dir_count += 1
            elif item.is_file():
                files.append(item)
            else:
                assert item.is_symlink()
                self._db.record_symlink(abs_item, os.readlink(abs_item))
                symlink_count += 1
        for item in files:
            self._process_file(item)
        self._db.record_directory(
            path, len(files), dir_count, symlink_count)

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

    def __init__(self, db: DB, que: queue.Queue):
        self._db = db
        self._queue = que


def md5(fname: str):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def main():
    parser = argparse.ArgumentParser(
        description='Picture collection reorganisation tool')
    parser.add_argument('paths', nargs='+', type=str)
    parser.add_argument('--db', type=str, default='reorg.db')
    parser.add_argument('--workers', type=int, default=os.cpu_count() * 2)
    parser.add_argument('--threading', type=bool, default=False)
    parser.add_argument('--queue_length', type=int, default=0)
    args = parser.parse_args(sys.argv)
    if args.threading:
        que = queue.Queue(maxsize=args.queue_length)
        dque = queue.Queue(maxsize=args.queue_length)
    else:
        que = multiprocessing.Queue(maxsize=args.queue_length)
        dque = multiprocessing.Queue(maxsize=args.queue_length)
    db = DB(args.db, dque)
    scanner = Scanner(db, que)
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
