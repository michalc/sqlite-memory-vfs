# sqlite-memory-vfs

[![PyPI package](https://img.shields.io/pypi/v/sqlite-memory-vfs?label=PyPI%20package&color=%234c1)](https://pypi.org/project/sqlite-memory-vfs/) [![Test suite](https://img.shields.io/github/actions/workflow/status/michalc/sqlite-memory-vfs/test.yml?label=Test%20suite)](https://github.com/michalc/sqlite-memory-vfs/actions/workflows/test.yml) [![Code coverage](https://img.shields.io/codecov/c/github/michalc/sqlite-memory-vfs?label=Code%20coverage)](https://app.codecov.io/gh/michalc/sqlite-memory-vfs)

Python virtual filesystem for SQLite to read from and write to memory.

While SQLite supports the special filename `:memory:` that allows the creation of empty databases in memory, `sqlite_deserialize` allows the population of an in-memory database from raw bytes of a serialized database, and `sqlite_serialize` allows the extraction of the raw bytes of an in-memory database, there are limitations.

- The function `sqlite_deserialize` cannot populate a database from non-contiguous raw bytes.
- The function `sqlite_serialize` cannot serialize to non-contiguous bytes.
- Both of these functions only work with databases that are less than 2GB in total, because [SQLite will not allocate more than 2GB in one go](https://www.sqlite.org/malloc.html).

This virtual filesystem overcomes these limitations. Specifically it allows larger databases to be downloaded and queried without hitting disk, and it allows larger databases to be generated and uploaded without hitting disk.

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
from contextlib import closing
import apsw
import httpx
import sqlite_memory_vfs

memory_vfs = sqlite_memory_vfs.MemoryVFS()

# Any iterable of bytes can be used. In this example, they come via HTTP
url = "https://data.api.trade.gov.uk/v1/datasets/uk-trade-quotas/versions/v1.0.366/data?format=sqlite"
with \
        httpx.stream("GET", url) as r, \
        closing(apsw.Connection('quota_balances.sqlite', vfs=memory_vfs.name)) as db:

    memory_vfs.deserialize_iter(db, r.iter_bytes())

    cursor = db.cursor()
    cursor.execute('SELECT * FROM quotas;')
    print(cursor.fetchall())
```

If the `deserialize_iter` step is ommitted an empty database is automatically created in memory.

See the [APSW documentation](https://rogerbinns.github.io/apsw/) for more usage examples.


### Serializing (getting a regular SQLite file out of the VFS)

The bytes corresponding to each SQLite database in the VFS can be extracted with the `serialize_iter` function, which returns an iterable of `bytes`

```python
with \
        open('my_db.sqlite', 'wb') as f, \
        closing(apsw.Connection('quota_balances.sqlite', vfs=memory_vfs.name)) as db:

    for chunk in memory_vfs.serialize_iter(db):
        f.write(chunk)
```


## Concurrency

It should be safe for any number of readers and writers to attempt to access the database - locking is implemented by the VFS which blocks access to the database when a write in in-flight.

If connection gets blocked, then it will raise `apsw.BusyError`. This is normal SQLite behaviour. You can request that SQLite retry certain actions automatically for a period of time to try to reduce the chance that this surfaces to your code. This can be done by setting a [busy timeout](https://www.sqlite.org/pragma.html#pragma_busy_timeout), for example to set a 500 millisecond timeout:

```sql
PRAGMA busy_timeout = 500;
```

Under the hood [writer starvation](https://www.sqlite.org/lockingv3.html#writer_starvation) is avoided by the use of a PENDING lock, much like the default SQLite VFS that writes to disk.


### Comparison with `sqlite_deserialize`

The main reason for using sqlite-memory-vfs over `sqlite_deserialize` is the lower memory usage for larger databases. For example the following may not even complete due to not being able to allocate enough contiguous memory for the database:

```python
import resource
from contextlib import closing

import apsw
import httpx

url = "https://data.api.trade.gov.uk/v1/datasets/uk-tariff-2021-01-01/versions/v4.0.46/data?format=sqlite"

with closing(apsw.Connection(':memory:')) as db:
    db.deserialize('main', httpx.get(url).read())
    cursor = db.cursor()
    cursor.execute('SELECT * FROM measures;')
    print(cursor.fetchall())

print('Max memory usage:', resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
```

But the following does / should output a lower value of memory usage:

```python
import resource
from contextlib import closing

import apsw
import httpx
import sqlite_memory_vfs

url = "https://data.api.trade.gov.uk/v1/datasets/uk-tariff-2021-01-01/versions/v4.0.46/data?format=sqlite"
memory_vfs = sqlite_memory_vfs.MemoryVFS()

with httpx.stream("GET", url) as r:
    memory_vfs.deserialize_iter('tariff.sqlite', r.iter_bytes())

with closing(apsw.Connection('tariff.sqlite', vfs=memory_vfs.name)) as db:
    cursor = db.cursor()
    cursor.execute('SELECT count(*) FROM measures;')
    print(cursor.fetchall())

print('Max memory usage:', resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
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
