import os
import tempfile
import threading
import time
import uuid
from contextlib import closing, contextmanager

import apsw
import sqlite3
import pytest

from sqlite_memory_vfs import MemoryVFS

PAGE_SIZES = [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536]
BLOCK_SIZES = [4095, 4096, 4097, 1000000]
JOURNAL_MODES = ['DELETE', 'TRUNCATE', 'PERSIST', 'MEMORY', 'OFF']


@contextmanager
def transaction(cursor):
    cursor.execute('BEGIN;')
    try:
        yield cursor
    except:
        cursor.execute('ROLLBACK;')
        raise
    else:
        cursor.execute('COMMIT;')


def set_pragmas(cursor, page_size, journal_mode):
    sqls = [
        f'PRAGMA page_size = {page_size};',
        f'PRAGMA journal_mode = {journal_mode};',
    ]
    for sql in sqls:
        cursor.execute(sql)


def create_db(cursor):
    sqls = [
        'CREATE TABLE foo(x,y);',
        'INSERT INTO foo VALUES ' + ','.join('(1,2)' for _ in range(0, 100)) + ';',
    ] + [
        f'CREATE TABLE foo_{i}(x,y);' for i in range(0, 10)
    ]
    for sql in sqls:
        cursor.execute(sql)


def empty_db(cursor):
    sqls = [
        'DROP TABLE foo;'
    ] + [
        f'DROP TABLE foo_{i};' for i in range(0, 10)
    ]
    for sql in sqls:
        cursor.execute(sql)


@pytest.mark.parametrize(
    'page_size', PAGE_SIZES
)
@pytest.mark.parametrize(
    'journal_mode', JOURNAL_MODES
)
def test_memory_vfs(page_size, journal_mode):
    memory_vfs = MemoryVFS()

    # Create a database and query it
    with closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db:
        set_pragmas(db.cursor(), page_size, journal_mode)

        with transaction(db.cursor()) as cursor:
            create_db(cursor)

        cursor.execute('SELECT * FROM foo;')
        assert cursor.fetchall() == [(1, 2)] * 100

        cursor.execute('PRAGMA integrity_check;')
        assert cursor.fetchall() == [('ok',)]

    # Query an existing database
    with \
            closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db, \
            transaction(db.cursor()) as cursor:

        cursor = db.cursor()
        cursor.execute('SELECT * FROM foo;')

        assert cursor.fetchall() == [(1, 2)] * 100

        cursor.execute('PRAGMA integrity_check;')
        assert cursor.fetchall() == [('ok',)]

    # Serialize a database with serialize_iter and query it
    with \
            tempfile.NamedTemporaryFile() as fp_memory_vfs, \
            tempfile.NamedTemporaryFile() as fp_sqlite3:

        for chunk in memory_vfs.serialize_iter('a-test/cool.db'):
            # Empty chunks can be treated as EOF, so never output those
            assert bool(chunk)
            fp_memory_vfs.write(chunk)

        fp_memory_vfs.flush()

        with \
                closing(sqlite3.connect(fp_memory_vfs.name)) as db, \
                transaction(db.cursor()) as cursor:

            cursor.execute('SELECT * FROM foo;')
            assert cursor.fetchall() == [(1, 2)] * 100

            cursor.execute('PRAGMA integrity_check;')
            assert cursor.fetchall() == [('ok',)]

        # Serialized form should be the same length as one constructed without the VFS...
        with closing(sqlite3.connect(fp_sqlite3.name)) as db:
            set_pragmas(db.cursor(), page_size, journal_mode)

            with transaction(db.cursor()) as cursor:
                create_db(cursor)

            cursor.execute('PRAGMA integrity_check;')
            assert cursor.fetchall() == [('ok',)]

        assert os.path.getsize(fp_memory_vfs.name) == os.path.getsize(fp_sqlite3.name)

        # ...including after a VACUUM (which cannot be in a transaction)
        with closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db:
            with transaction(db.cursor()) as cursor:
                empty_db(cursor)
            db.cursor().execute('VACUUM;')

            cursor.execute('PRAGMA integrity_check;')
            assert cursor.fetchall() == [('ok',)]

        fp_memory_vfs.truncate(0)
        fp_memory_vfs.seek(0)

        for chunk in memory_vfs.serialize_iter('a-test/cool.db'):
            assert bool(chunk)
            fp_memory_vfs.write(chunk)

        fp_memory_vfs.flush()

        with closing(sqlite3.connect(fp_sqlite3.name)) as db:
            with transaction(db.cursor()) as cursor:
                empty_db(cursor)

            db.cursor().execute('VACUUM;')

            cursor.execute('PRAGMA integrity_check;')
            assert cursor.fetchall() == [('ok',)]

        assert os.path.getsize(fp_memory_vfs.name) == os.path.getsize(fp_sqlite3.name)


@pytest.mark.parametrize(
    'page_size', PAGE_SIZES
)
@pytest.mark.parametrize(
    'block_size', BLOCK_SIZES
)
@pytest.mark.parametrize(
    'journal_mode', JOURNAL_MODES
)
def test_deserialize_iter(page_size, block_size, journal_mode):
    memory_vfs = MemoryVFS()

    with tempfile.NamedTemporaryFile() as fp_sqlite3:
        with closing(sqlite3.connect(fp_sqlite3.name)) as db:
            set_pragmas(db.cursor(), page_size, journal_mode)

            with transaction(db.cursor()) as cursor:
                create_db(cursor)
                cursor.executemany('INSERT INTO foo VALUES (?,?);', ((1,2) for _ in range(0, 30000)))

        memory_vfs.deserialize_iter('another-test/cool.db', bytes_iter=iter(lambda: fp_sqlite3.read(block_size), b''))

    with \
            closing(apsw.Connection('another-test/cool.db', vfs=memory_vfs.name)) as db, \
            transaction(db.cursor()) as cursor:

        cursor = db.cursor()
        cursor.execute('SELECT * FROM foo;')
        assert cursor.fetchall() == [(1, 2)] * 30100

        cursor.execute('PRAGMA integrity_check;')
        assert cursor.fetchall() == [('ok',)]

        cursor.execute('UPDATE foo SET x = 0, y = 0')
        cursor.execute('PRAGMA integrity_check;')
        assert cursor.fetchall() == [('ok',)]

        cursor.execute('SELECT * FROM foo;')
        assert cursor.fetchall() == [(0, 0)] * 30100


@pytest.mark.parametrize(
    'page_size', [65536]
)
def test_byte_lock_page(page_size):
    memory_vfs = MemoryVFS()
    empty = (bytes(4050),)

    with closing(apsw.Connection('another-test/cool.db', vfs=memory_vfs.name)) as db:
        db.cursor().execute(f'PRAGMA page_size = {page_size};')

        with transaction(db.cursor()) as cursor:
            cursor.execute('CREATE TABLE foo(content BLOB);')
            cursor.executemany('INSERT INTO foo VALUES (?);', (empty for _ in range(0, 300000)))

        cursor.execute('SELECT * FROM foo LIMIT 1;')
        assert cursor.fetchall() == [empty]

        cursor.execute('DELETE FROM foo;')
        cursor.execute('SELECT * FROM foo LIMIT 1;')
        assert cursor.fetchall() == [] 

        with transaction(db.cursor()) as cursor:
            cursor.executemany('INSERT INTO foo VALUES (?);', (empty for _ in range(0, 300000)))

        cursor.execute('SELECT * FROM foo LIMIT 1;')
        assert cursor.fetchall() == [empty]


def test_set_temp_store_which_calls_xaccess():
    memory_vfs = MemoryVFS()
    with closing(apsw.Connection('another-test/cool.db', vfs=memory_vfs.name)) as db:
        db.cursor().execute("pragma temp_store_directory = 'my-temp-store'")


@pytest.mark.parametrize(
    'page_size', [4096]
)
@pytest.mark.parametrize(
    'journal_mode', [journal_mode for journal_mode in JOURNAL_MODES if journal_mode != 'OFF']
)
def test_rollback(page_size, journal_mode):
    memory_vfs = MemoryVFS()

    with closing(apsw.Connection('another-test/cool.db', vfs=memory_vfs.name)) as db:
        db.cursor().execute(f'PRAGMA page_size = {page_size};')
        db.cursor().execute(f'PRAGMA journal_mode = {journal_mode};')
        db.cursor().execute('CREATE TABLE foo(content text);')

        try:
            with transaction(db.cursor()) as cursor:
                cursor.execute("INSERT INTO foo VALUES ('hello');");
                cursor.execute('SELECT * FROM foo;')
                assert cursor.fetchall() == [('hello',)]
                raise Exception()
        except:
            cursor.execute('SELECT * FROM foo;')
            assert cursor.fetchall() == []

        cursor.execute("INSERT INTO foo VALUES ('hello');");
        cursor.execute('SELECT * FROM foo;')
        assert cursor.fetchall() == [('hello',)]

        try:
            with transaction(db.cursor()) as cursor:
                cursor.execute("UPDATE foo SET content='goodbye'");
                cursor.execute('SELECT * FROM foo;')
                assert cursor.fetchall() == [('goodbye',)]
                raise Exception()
        except:
            cursor.execute('SELECT * FROM foo;')
            assert cursor.fetchall() == [('hello',)]


@pytest.mark.parametrize(
    'page_size', PAGE_SIZES
)
@pytest.mark.parametrize(
    'journal_mode', JOURNAL_MODES
)
def test_transaction_non_exclusive(page_size, journal_mode):
    memory_vfs = MemoryVFS()

    with \
        closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_1, \
        closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_2:
        set_pragmas(db_1.cursor(), page_size, journal_mode)

        # Create the database
        with transaction(db_1.cursor()) as cursor_1:
            create_db(cursor_1)

            cursor_1.execute('SELECT * FROM foo;')
            assert cursor_1.fetchall() == [(1, 2)] * 100

        with \
                transaction(db_1.cursor()) as cursor_1, \
                transaction(db_2.cursor()) as cursor_2:

            # Multiple transactions can query it...
            cursor_1.execute('SELECT * FROM foo;')
            assert cursor_1.fetchall() == [(1, 2)] * 100

            cursor_2.execute('SELECT * FROM foo;')
            assert cursor_2.fetchall() == [(1, 2)] * 100

            # ... but once modifications are made
            cursor_1.execute('DELETE FROM foo;')

            # ... concurrent queries are still possible
            cursor_2.execute('SELECT * FROM foo;')
            assert cursor_2.fetchall() == [(1, 2)] * 100

            # but not writes
            with pytest.raises(apsw.BusyError):
                cursor_2.execute('DELETE FROM foo;')


@pytest.mark.parametrize(
    'page_size', PAGE_SIZES
)
@pytest.mark.parametrize(
    'journal_mode', JOURNAL_MODES
)
def test_transaction_exclusive(page_size, journal_mode):
    memory_vfs = MemoryVFS()

    with \
        closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_1, \
        closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_2:
        set_pragmas(db_1.cursor(), page_size, journal_mode)

        # Create the database
        with transaction(db_1.cursor()) as cursor_1:
            create_db(cursor_1)

            cursor_1.execute('SELECT * FROM foo;')
            assert cursor_1.fetchall() == [(1, 2)] * 100

        # And an exclusive lock...
        cursor_1 = db_1.cursor()
        cursor_1.execute('BEGIN EXCLUSIVE;')

        # .. prevents concurrent reads
        cursor_2 = db_2.cursor()
        with pytest.raises(apsw.BusyError):
            cursor_2.execute('SELECT * FROM foo;')


@pytest.mark.parametrize(
    'page_size', PAGE_SIZES
)
@pytest.mark.parametrize(
    'journal_mode', JOURNAL_MODES
)
def test_transaction_reading_prevents_exclusive(page_size, journal_mode):
    memory_vfs = MemoryVFS()

    with \
        closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_1, \
        closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_2:
        set_pragmas(db_1.cursor(), page_size, journal_mode)

        # Create the database
        with transaction(db_1.cursor()) as cursor_1:
            create_db(cursor_1)

            cursor_1.execute('SELECT * FROM foo;')
            assert cursor_1.fetchall() == [(1, 2)] * 100

        # Starting to read
        cursor_1 = db_1.cursor()
        cursor_1.execute('SELECT * FROM foo')

        # .. prevents getting an exclusive lock
        cursor_2 = db_2.cursor()
        with pytest.raises(apsw.BusyError):
            cursor_2.execute('BEGIN EXCLUSIVE;')

    # And do the same thing on new database connections
    with \
        closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_1, \
        closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_2:

        # Starting to read
        cursor_1 = db_1.cursor()
        cursor_1.execute('SELECT * FROM foo')

        # .. prevents getting an exclusive lock
        cursor_2 = db_2.cursor()
        with pytest.raises(apsw.BusyError):
            cursor_2.execute('BEGIN EXCLUSIVE;')


@pytest.mark.parametrize(
    'page_size', PAGE_SIZES
)
@pytest.mark.parametrize(
    'journal_mode', JOURNAL_MODES
)
def test_transaction_can_start_read_if_another_transaction_started(page_size, journal_mode):
    memory_vfs = MemoryVFS()

    with \
        closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_1, \
        closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_2:
        set_pragmas(db_1.cursor(), page_size, journal_mode)

        # Create the database
        with transaction(db_1.cursor()) as cursor_1:
            create_db(cursor_1)

            cursor_1.execute('SELECT * FROM foo;')
            assert cursor_1.fetchall() == [(1, 2)] * 100

        with transaction(db_1.cursor()) as cursor_1:
            # Obtains a reserved lock
            cursor_1.execute('DELETE FROM foo;')

            # Obtains a reader lock...
            cursor_2 = db_2.cursor()
            cursor_2.execute('SELECT * FROM foo')
            # ... and then drops the reader lock
            cursor_2.fetchall()


@pytest.mark.parametrize(
    'page_size', PAGE_SIZES
)
@pytest.mark.parametrize(
    'journal_mode', JOURNAL_MODES
)
def test_transaction_interrupted(page_size, journal_mode):
    memory_vfs = MemoryVFS()

    with closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_1:
        set_pragmas(db_1.cursor(), page_size, journal_mode)

        with transaction(db_1.cursor()) as cursor_1:
            create_db(cursor_1)

        cursor_1 = db_1.cursor()
        cursor_1.execute('BEGIN;')
        cursor_1.executemany('INSERT INTO foo VALUES (?,?);', ((1,2,) for _ in range(0, 300000)))

    with closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_1:
        cursor_1 = db_1.cursor()
        cursor_1.execute('SELECT * FROM foo')


@pytest.mark.parametrize(
    'page_size', PAGE_SIZES
)
@pytest.mark.parametrize(
    'journal_mode', [journal_mode for journal_mode in JOURNAL_MODES if journal_mode not in ('OFF', 'MEMORY')]
)
def test_transaction_interrupted_with_hot_journal(page_size, journal_mode):
    memory_vfs = MemoryVFS()

    with closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_1:
        set_pragmas(db_1.cursor(), page_size, journal_mode)

        with transaction(db_1.cursor()) as cursor_1:
            create_db(cursor_1)

        cursor_1 = db_1.cursor()
        cursor_1.execute('BEGIN;')
        cursor_1.executemany('INSERT INTO foo VALUES (?,?);', ((1,2,) for _ in range(0, 300000)))

        # Manually extract the bytes of the journal. We don't use the serialize API because it
        # attempts to lock the file using SQLite, which doesn't work since this isn't a database
        hot_journal_tuple = memory_vfs.databases["a-test/cool.db-journal"]
        hot_journal = b''.join(hot_journal_tuple[0].values())

    hot_journal_tuple[0].clear()
    hot_journal_tuple[0][0] = hot_journal
    memory_vfs.databases["a-test/cool.db-journal"] = hot_journal_tuple

    # This makes sure that RESERVED locking logic is good after a hot journal recovery, because
    # during a hot-journal recovery SHARED locks go straight to EXCLUSIVE, bypassing RESERVED.
    # Specifically, it should not be possible for two connections to start a transaction
    with \
        closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_1, \
        closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_2:

        cursor_1 = db_1.cursor()
        cursor_1.execute('BEGIN IMMEDIATE')
        cursor_2 = db_2.cursor()

        with pytest.raises(apsw.BusyError):
            cursor_2.execute('BEGIN IMMEDIATE')


@pytest.mark.parametrize(
    'page_size', PAGE_SIZES
)
@pytest.mark.parametrize(
    'journal_mode', [journal_mode for journal_mode in JOURNAL_MODES if journal_mode not in ('OFF', 'MEMORY')]
)
def test_writer_starvation_avoided(page_size, journal_mode):
    memory_vfs = MemoryVFS()
    writer_complete = False
    barrier = threading.Barrier(2)

    def writer():
        nonlocal writer_complete
        with closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_3:
            cursor_writer = db_3.cursor()
            cursor_writer.execute('PRAGMA busy_timeout=10000')
            barrier.wait()
            cursor_writer.execute('BEGIN EXCLUSIVE')
        writer_complete = True

    with \
        closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_1, \
        closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_2:
        set_pragmas(db_1.cursor(), page_size, journal_mode)

        with transaction(db_1.cursor()) as cursor:
            create_db(cursor)

        cursor_reader_1 = db_1.cursor()
        cursor_reader_1.execute('SELECT * FROM foo')

        t = threading.Thread(target=writer)
        t.start()

        barrier.wait()
        time.sleep(0.1)  # Just enough time for BEGIN EXLUSIVE to run in SQLite in the thead

        # We make sure we cannot get a new reader
        cursor_reader_2 = db_2.cursor()
        with pytest.raises(apsw.BusyError):
            cursor_reader_2.execute('SELECT * FROM foo')

        # But the first reader completes
        cursor_reader_1.fetchall()

        # And the writer also completes
        t.join()
        assert writer_complete


@pytest.mark.parametrize(
    'page_size', PAGE_SIZES
)
@pytest.mark.parametrize(
    'journal_mode', JOURNAL_MODES
)
def test_serialization_blocks_writes_and_not_reads(page_size, journal_mode):
    memory_vfs = MemoryVFS()

    with closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_1:
        set_pragmas(db_1.cursor(), page_size, journal_mode)

        with transaction(db_1.cursor()) as cursor_1:
            create_db(cursor_1)

    for chunk in memory_vfs.serialize_iter("a-test/cool.db"):
        with closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_1:
            cursor_1 = db_1.cursor()
            with pytest.raises(apsw.BusyError):
                cursor_1.execute('BEGIN EXCLUSIVE')
            cursor_1.execute('SELECT * FROM foo')
            cursor_1.fetchall()
        break


@pytest.mark.parametrize(
    'page_size', PAGE_SIZES
)
@pytest.mark.parametrize(
    'journal_mode', JOURNAL_MODES
)
def test_write_blocks_serialization(page_size, journal_mode):
    memory_vfs = MemoryVFS()

    with closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_1:
        set_pragmas(db_1.cursor(), page_size, journal_mode)

        with transaction(db_1.cursor()) as cursor_1:
            create_db(cursor_1)

    with closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_1:
        cursor_1 = db_1.cursor()
        cursor_1.execute('BEGIN EXCLUSIVE')

        with pytest.raises(apsw.BusyError):
            next(iter(memory_vfs.serialize_iter("a-test/cool.db")))


@pytest.mark.parametrize(
    'page_size', PAGE_SIZES
)
@pytest.mark.parametrize(
    'journal_mode', JOURNAL_MODES
)
def test_read_blocks_deserialization(page_size, journal_mode):
    memory_vfs = MemoryVFS()

    with closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_1:
        set_pragmas(db_1.cursor(), page_size, journal_mode)

        with transaction(db_1.cursor()) as cursor_1:
            create_db(cursor_1)

    serialized = list(memory_vfs.serialize_iter("a-test/cool.db"))

    with closing(apsw.Connection("a-test/cool.db", vfs=memory_vfs.name)) as db_1:
        cursor_1 = db_1.cursor()
        # Obtains a SHARED lock
        cursor_1.execute("SELECT 1 FROM sqlite_master")

        with pytest.raises(apsw.BusyError):
            memory_vfs.deserialize_iter("a-test/cool.db", serialized)

        # Drops the SHARED lock
        cursor_1.fetchall()

        memory_vfs.deserialize_iter("a-test/cool.db", serialized)
