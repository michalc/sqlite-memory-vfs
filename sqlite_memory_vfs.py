import threading
import uuid

import apsw
from sortedcontainers import SortedDict


class MemoryVFS(apsw.VFS):        
    def __init__(self):
        self.name = f'memory-vfs-{str(uuid.uuid4())}'
        self.databases = {}
        self.databases_lock = threading.Lock()
        super().__init__(name=self.name, base='')

    def xAccess(self, pathname, flags):
        with self.databases_lock:
            return (
                flags == apsw.mapping_access["SQLITE_ACCESS_EXISTS"]
                and pathname in self.databases
            ) or (
                flags != apsw.mapping_access["SQLITE_ACCESS_EXISTS"]
            )

    def xFullPathname(self, filename):
        return filename

    def xDelete(self, filename, syncdir):
        with self.databases_lock:
            del self.databases[filename]

    def xOpen(self, filename, flags):
        name = filename.filename() if isinstance(filename, apsw.URIFilename) else \
            filename

        with self.databases_lock:
            try:
                db, lock, locks = self.databases[name]
            except KeyError:
                db, lock, locks = (SortedDict(), threading.Lock(), {
                    apsw.SQLITE_LOCK_SHARED: 0,
                    apsw.SQLITE_LOCK_RESERVED: 0,
                    apsw.SQLITE_LOCK_PENDING: 0,
                    apsw.SQLITE_LOCK_EXCLUSIVE: 0,
                })
                if name is not None:
                    self.databases[name] = (db, lock, locks)

        return MemoryVFSFile(db, lock ,locks)

    def serialize_iter(self, conn):
        cursor = conn.cursor()

        cursor.execute('PRAGMA database_list')
        filename = next(iter(_filename for _, name, _filename in cursor.fetchall() if name == 'main'))

        # Obtains a shared lock that prevents writes during the serialization
        cursor.execute('SELECT 1 FROM sqlite_master')

        with self.databases_lock:
            db, _, _ = self.databases[filename]

        try:
            yield from db.values()
        finally:
            # Release the shared lock
            cursor.fetchall()

    def deserialize_iter(self, conn, bytes_iter):
        db = SortedDict()

        i = 0
        for b in bytes_iter:
            db[i] = b
            i += len(b)

        cursor = conn.cursor()

        cursor.execute('PRAGMA database_list')
        filename = next(iter(_filename for _, name, _filename in cursor.fetchall() if name == 'main'))

        # Obtain an exclusive lock that prevents reads and writes during the replace of an
        # existing database if there was one. Note that the iteration is done outside of the
        # lock to minimise the time that the lock is needed
        cursor.execute('BEGIN EXCLUSIVE')
        with self.databases_lock:
            self.databases[filename] = (db,) + self.databases[filename][1:]

        # Release the exclusive lock
        cursor.execute('ROLLBACK')


class MemoryVFSFile():
    def __init__(self, db, lock, locks):
        # The bytes of the database
        self._db = db

        # A Python lock object that makes sure atomicy when...
        self._lock = lock
        # ... the SQLite locks on the specific file are accessed
        self._locks = locks

        self._level = apsw.SQLITE_LOCK_NONE

    def _blocks(self, offset, amount):
        db = self._db

        index = max(db.bisect_left(offset) - 1, 0)
        while amount > 0:
            try:
                block_offset, block = db.peekitem(index)
            except IndexError:
                return

            if block_offset > offset:
                return

            start = offset - block_offset
            consume = min(len(block) - start, amount)
            yield (block_offset, block, start, consume)
            amount -= consume
            offset += consume
            index += 1

    def xRead(self, amount, offset):
        return b''.join(
            block[start:start+consume]
            for _, block, start, consume in self._blocks(offset, amount)
        )

    def xFileControl(self, *args):
        return False

    def xCheckReservedLock(self):
        with self._lock:
            return self._locks[apsw.SQLITE_LOCK_RESERVED]

    def xLock(self, level):
        with self._lock:
            if self._level == level:
                return

            # SHARED cannot be obtained if the file has a PENDING lock
            if level == apsw.SQLITE_LOCK_SHARED and self._locks[apsw.SQLITE_LOCK_PENDING]:
                raise apsw.BusyError()

            # SHARED cannot be obtained if the file has an EXCLUSIVE lock
            if level == apsw.SQLITE_LOCK_SHARED and self._locks[apsw.SQLITE_LOCK_EXCLUSIVE]:
                raise apsw.BusyError()

            # RESERVED cannot be obtained if the file has a RESERVED lock
            if level == apsw.SQLITE_LOCK_RESERVED and self._locks[apsw.SQLITE_LOCK_RESERVED]:
                raise apsw.BusyError()

            # EXCLUSIVE cannot be obtained if there is more than one SHARED. But if we're not
            # already PENDING, then we can actually obtain PENDING which then allows the client
            # to retry (for example via `PRAGMA busy_timeout;`), and preventing other clients
            # from obtaining new SHARED locks, preventing writer starvation
            if level == apsw.SQLITE_LOCK_EXCLUSIVE and self._locks[apsw.SQLITE_LOCK_SHARED] > 1:
                if not self._locks[apsw.SQLITE_LOCK_PENDING]:
                    self._locks[apsw.SQLITE_LOCK_PENDING] += 1
                    self._level = apsw.SQLITE_LOCK_PENDING
                raise apsw.BusyError()

            self._locks[level] += 1
            self._level = level

    def xUnlock(self, level):
        with self._lock:
            if self._level == level:
                return

            for lock_level in self._locks:
                if self._level >= lock_level and level < lock_level:
                    # Without the max against zero we would have negative locks for RESERVED after
                    # the case where a SHARED lock goes straight to EXCLUSIVE, bypassing RESERVED
                    # or PENDING. Bypassing PENDING happens very regularly when there is no
                    # contention, and bypassing RESERVED happens when we have a hot-journal
                    self._locks[lock_level] = max(self._locks[lock_level] - 1, 0)

            self._level = level

    def xClose(self):
        pass

    def xFileSize(self):
        try:
            final_offset, final_block = self._db.peekitem(-1)
        except IndexError:
            return 0
        else:
            return final_offset + len(final_block)

    def xSync(self, flags):
        return True

    def xSectorSize(self):
        return 0

    def xTruncate(self, newsize):
        db = self._db

        for block_offset, block in reversed(db.items()):
            to_keep = max(newsize - block_offset, 0)
            if to_keep == 0:
                del db[block_offset]
            elif to_keep < len(block):
                db[block_offset] = block[:to_keep]

        return True

    def xWrite(self, data, offset):
        db = self._db

        # Mostly SQLite populates data in order, but in at least two cases it doesn't:
        # - The lock page on the main file at offset 1073741824 is skipped
        # - Journal files can skip bytes
        # If we have skipped, we add in a block of empty bytes to make sure all the logic works
        # in terms of reading and writing, that assumes we have all the bytes
        try:
            final_offset, final_block = db.peekitem(-1)
        except IndexError:
            pass
        else:
            size = final_offset + len(final_block)
            if offset >= size:
                db[size] = bytes(offset - size)

        # We might need to delete or modify blocks because they were populated not on exact page
        # boundaries during initial deserialisation. To avoid issues due to modifying the list
        # while iterating, we gather the blocks to modify or delete in a list. There is at most a
        # page of data, so it shouldn't be too many
        blocks_to_delete = list(self._blocks(offset, len(data)))
        for (block_offset, block, start, consume) in blocks_to_delete:
            left_block_offset = block_offset
            left_keep = block[:start]

            right_block_offset = block_offset + start + consume
            right_keep = block[start+consume:]

            if left_keep:
                db[left_block_offset] = left_keep
            else:
                del db[block_offset]

            if right_keep:
                db[right_block_offset] = right_keep

        db[offset] = data
