import threading
import uuid

import apsw
from sortedcontainers import SortedDict


class MemoryVFS(apsw.VFS):        
    def __init__(self):
        self.name = f'memory-vfs-{str(uuid.uuid4())}'
        self.databases = {}
        self.databases_lock = threading.RLock()
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
                db, lock, locks = self.deserialize_iter(name, ())

        return MemoryVFSFile(db, lock ,locks)

    def serialize_iter(self, filename):
        with self.databases_lock:
            db, _, _ = self.databases[filename]

        yield from db.values()

    def deserialize_iter(self, name, bytes_iter):
        db = SortedDict()

        i = 0
        for b in bytes_iter:
            db[i] = b
            i += len(b)

        with self.databases_lock:
            self.databases[name] = db, threading.Lock(), {
                apsw.SQLITE_LOCK_SHARED: 0,
                apsw.SQLITE_LOCK_RESERVED: 0,
                apsw.SQLITE_LOCK_EXCLUSIVE: 0,
            }
            return self.databases[name]


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

            # SHARED cannot be obtained if the file has any EXCLUSIVE
            if level == apsw.SQLITE_LOCK_SHARED and self._locks[apsw.SQLITE_LOCK_EXCLUSIVE]:
                raise apsw.BusyError()

            # RESERVED cannot be obtained if there is another RESERVED
            if level == apsw.SQLITE_LOCK_RESERVED and self._locks[apsw.SQLITE_LOCK_RESERVED]:
                raise apsw.BusyError()

            # EXCLUSIVE cannot be obtained if there are more than one SHARED
            if level == apsw.SQLITE_LOCK_EXCLUSIVE and self._locks[apsw.SQLITE_LOCK_SHARED] > 1:
                raise apsw.BusyError()

            self._locks[level] += 1
            self._level = level

    def xUnlock(self, level):
        with self._lock:
            if self._level == level:
                return

            for lock_level in self._locks:
                if self._level >= lock_level and level < lock_level:
                    self._locks[lock_level] -= 1

            self._level = level

    def xClose(self):
        pass

    def xFileSize(self):
        return sum(len(b) for b in self._db.values())

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
        lock_page_offset = 1073741824
        page_size = len(data)
        db = self._db

        # SQLite seems to always write pages sequentially, except that it skips the byte-lock page.
        # To make sure serialization works, we populate the lock page with null bytes if we know
        # we're just after it.
        just_after_lock_page = offset == lock_page_offset + page_size
        to_populate = \
            (((lock_page_offset, bytes(page_size)),) if just_after_lock_page else ()) + \
            ((offset, data),)

        for offset_to_populate, page_to_populate in to_populate:
            # We might need to delete or modify blocks because they were populated not on exact
            # page boundaries during initial deserialisation. To avoid issues due to modifying the
            # list while iterating, we gather the blocks to modify or delete in a list. Each
            # iteration there is at most a page of data, so it shouldn't be too many
            blocks_to_delete = list(self._blocks(offset_to_populate, len(page_to_populate)))
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

            db[offset_to_populate] = page_to_populate
