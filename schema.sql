create table files (
    abs_path text primary key,
    base_name text,
    dir_name text,
    extension text,
    size int,
    mtime timestamp,
    md5_hex text
);

create table directories (
    abs_path text primary key,
    file_count int,
    dir_count int,
    symlink_count int,
    last_scanned timestamp
);

create table symlinks (
    abs_path text primary key,
    target text    
);

create index file_md5_hex on files (md5_hex);