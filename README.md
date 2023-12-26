# sqlite-memory-vfs

[![PyPI package](https://img.shields.io/pypi/v/sqlite-s3vfs?label=PyPI%20package&color=%234c1)](https://pypi.org/project/sqlite-s3vfs/) [![Test suite](https://img.shields.io/github/actions/workflow/status/uktrade/sqlite-s3vfs/test.yml?label=Test%20suite)](https://github.com/uktrade/sqlite-s3vfs/actions/workflows/test.yml) [![Code coverage](https://img.shields.io/codecov/c/github/uktrade/sqlite-s3vfs?label=Code%20coverage)](https://app.codecov.io/gh/uktrade/sqlite-s3vfs)

Python virtual filesystem for SQLite to read from and write to memory.

While SQLite supports the special filename `:memory:` that allows the creation of databases in memory, there is no way to populate such a database using raw bytes without hitting disk as an intermediate step. This virtual filesystem overcomes that limitation.

Based on [simonwo's gist](https://gist.github.com/simonwo/b98dc75feb4b53ada46f224a3b26274c), and inspired by [phiresky's sql.js-httpvfs](https://github.com/phiresky/sql.js-httpvfs), [dacort's Stack Overflow answer](https://stackoverflow.com/a/59434097/1319998), [michalc's sqlite-s3-query](https://github.com/michalc/sqlite-s3-query), and [uktrade's sqlite-s3vfs](https://github.com/uktrade/sqlite-s3vfs).

> Work in progress. This README is a rough design spec and so can contain information about features that do not yet exist, or information that's only applicable to [sqlite-s3vfs](https://github.com/uktrade/sqlite-s3vfs), its upstream source fork.


## Installation

sqlite-memory-vfs can be installed from PyPI using `pip`.

```bash
pip install sqlite-memory-vfs
```

This will automatically [APSW](https://rogerbinns.github.io/apsw/), and any of its dependencies.


## Usage

sqlite-s3vfs is an [APSW](https://rogerbinns.github.io/apsw/) virtual filesystem that requires [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) for its communication with S3.

```python
import apsw
import boto3
import sqlite_s3vfs

# A boto3 bucket resource
bucket = boto3.Session().resource('s3').Bucket('my-bucket')

# An S3VFS for that bucket
s3vfs = sqlite_s3vfs.S3VFS(bucket=bucket)

# sqlite-s3vfs stores many objects under this prefix
# Note that it's not typical to start a key prefix with '/'
key_prefix = 'my/path/cool.sqlite'

# Connect, insert data, and query
with apsw.Connection(key_prefix, vfs=s3vfs.name) as db:
    cursor = db.cursor()
    cursor.execute('''
        CREATE TABLE foo(x,y);
        INSERT INTO foo VALUES(1,2);
    ''')
    cursor.execute('SELECT * FROM foo;')
    print(cursor.fetchall())
```

See the [APSW documentation](https://rogerbinns.github.io/apsw/) for more examples.


### Serializing (getting a regular SQLite file out of the VFS)

The bytes corresponding to a regular SQLite file can be extracted with the `serialize_iter` function, which returns an iterable,

```python
for chunk in s3vfs.serialize_iter(key_prefix=key_prefix):
    print(chunk)
```

or with `serialize_fileobj`, which returns a non-seekable file-like object. This can be passed to Boto3's `upload_fileobj` method to upload a regular SQLite file to S3.

```python
target_obj = boto3.Session().resource('s3').Bucket('my-target-bucket').Object('target/cool.sqlite')
target_obj.upload_fileobj(s3vfs.serialize_fileobj(key_prefix=key_prefix))
```


### Deserializing (getting a regular SQLite file into the VFS)

```python
# Any iterable that yields bytes can be used. In this example, bytes come from
# a regular SQLite file already in S3
source_obj = boto3.Session().resource('s3').Bucket('my-source-bucket').Object('source/cool.sqlite')
bytes_iter = source_obj.get()['Body'].iter_chunks()

s3vfs.deserialize_iter(key_prefix='my/path/cool.sqlite', bytes_iter=bytes_iter)
```


### Block size and page size

SQLite writes data in _pages_, which are 4096 bytes by default. sqlite-s3vfs stores data in _blocks_, which are also 4096 bytes by default. If you change one you should change the other to match for performance reasons.

```python
s3vfs = sqlite_s3vfs.S3VFS(bucket=bucket, block_size=65536)
with apsw.Connection(key_prefix, vfs=s3vfs.name) as db:
    cursor = db.cursor()
    cursor.execute('''
        PRAGMA page_size = 65536;
    ''')
```


## Tests

The tests require the dev dependencies and MinIO started

```bash
pip install -e ".[dev]"
./start-minio.sh
```

can be run with pytest

```bash
pytest
```

and finally Minio stopped

```bash
./stop-minio.sh
```
