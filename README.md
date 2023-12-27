# sqlite-memory-vfs

[![PyPI package](https://img.shields.io/pypi/v/sqlite-s3vfs?label=PyPI%20package&color=%234c1)](https://pypi.org/project/sqlite-s3vfs/) [![Test suite](https://img.shields.io/github/actions/workflow/status/uktrade/sqlite-s3vfs/test.yml?label=Test%20suite)](https://github.com/uktrade/sqlite-s3vfs/actions/workflows/test.yml) [![Code coverage](https://img.shields.io/codecov/c/github/uktrade/sqlite-s3vfs?label=Code%20coverage)](https://app.codecov.io/gh/uktrade/sqlite-s3vfs)

Python virtual filesystem for SQLite to read from and write to memory.

While SQLite supports the special filename `:memory:` that allows the creation of databases in memory, there is no built-in way to populate such a database using raw bytes without hitting disk as an intermediate step. This virtual filesystem overcomes that limitation.

No locking is performed, so client code _must_ ensure that writes do not overlap with other writes or reads on the same database. If multiple writes happen at the same time, the database will probably become corrupt and data be lost.

Based on [simonwo's gist](https://gist.github.com/simonwo/b98dc75feb4b53ada46f224a3b26274c) and [uktrade's sqlite-s3vfs](https://github.com/uktrade/sqlite-s3vfs), and inspired by [phiresky's sql.js-httpvfs](https://github.com/phiresky/sql.js-httpvfs), [dacort's Stack Overflow answer](https://stackoverflow.com/a/59434097/1319998) and [michalc's sqlite-s3-query](https://github.com/michalc/sqlite-s3-query).


## Installation

sqlite-memory-vfs can be installed from PyPI using `pip`.

```bash
pip install sqlite-memory-vfs
```

This will automatically install [APSW](https://rogerbinns.github.io/apsw/) along with any other dependencies.


### Deserializing (getting a regular SQLite file into the VFS)

This library allows the raw bytes of a SQLite database to be queried without having to save it to disk. This can be done by using the `deserialize_iter` method of `MemoryVFS`, passing it an iterable of `bytes` instances that contain the SQLite database.

```python
import apsw
import httpx
import sqlite_memory_vfs

memory_vfs = sqlite_memory_vfs.MemoryVFS()

# Any iterable of bytes can be used. In this example, they come via HTTP
with httpx.stream("GET", "https://www.example.com/my_dq.sqlite") as r:
    memory_vfs.deserialize_iter('my_db.sqlite', r.iter_bytes())

with apsw.Connection('my_db.sqlite', vfs=memory_vfs.name) as db:
    cursor.execute('SELECT * FROM foo;')
    print(cursor.fetchall())
```

If the `deserialize_iter` step is ommitted an empty database is automatically created in memory.

See the [APSW documentation](https://rogerbinns.github.io/apsw/) for more usage examples.


### Serializing (getting a regular SQLite file out of the VFS)

The bytes corresponding to each SQLite database in the VFS can be extracted with the `serialize_iter` function, which returns an iterable of `bytes`

```python
with open('my_db.sqlite', 'wb') as f:
    for chunk in memory_vfs.serialize_iter('my_db.sqlite'):
        f.write(chunk)
```


## Tests

The tests require the dev dependencies installed

```bash
pip install -e ".[dev]"
```

and can then run with pytest

```bash
pytest
```
