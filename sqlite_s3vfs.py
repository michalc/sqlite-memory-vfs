import boto3
import apsw

BLOCK_SIZE = 64 * 1024
EMPTY_BLOCK = b"".join([b"\x00"] * BLOCK_SIZE)


class S3VFS(apsw.VFS):        
    def __init__(self, bucket, vfsname=f"s3vfs"):
        self.vfsname = vfsname
        self.bucket = bucket
        apsw.VFS.__init__(self, self.vfsname, base='')

    def xAccess(self, pathname, flags):
        if flags == apsw.mapping_access["SQLITE_ACCESS_EXISTS"]:
            return any(self.bucket.objects.filter(Prefix=pathname))
        elif flags == apsw.mapping_access["SQLITE_ACCESS_READWRITE"]:
            # something sometihng ACLs
            return True
        elif flags == apsw.mapping_access["SQLITE_ACCESS_READ"]:
            # something something ACLs
            return True

    def xDelete(self, filename, syncdir):
        self.bucket.objects.filter(Prefix=filename).delete()

    def xOpen(self, name, flags):
        return S3VFSFile(name, flags, self.bucket)


class S3VFSFile:
    def __init__(self, name, flags, bucket):
        if isinstance(name, apsw.URIFilename):
            self.key = name.filename()
        else:
            self.key = name
        self.bucket = bucket

    def blocks(self, offset, amount):
        while amount > 0:
            block = offset // BLOCK_SIZE  # which block to get
            start = offset % BLOCK_SIZE   # place in block to start
            consume = min(BLOCK_SIZE - start, amount)
            yield (block, start, consume)
            amount -= consume
            offset += consume

    def block_object(self, block):
        return self.bucket.Object(self.key + "/" + str(block))

    def block(self, block):
        try:
            data = self.block_object(block).get()["Body"].read()
        except self.bucket.meta.client.exceptions.NoSuchKey as e:
            data = EMPTY_BLOCK

        assert type(data) is bytes
        assert len(data) == BLOCK_SIZE
        return data

    def read(self, amount, offset):
        for block, start, consume in self.blocks(offset, amount):
            data = self.block(block)
            yield data[start:start+consume]

    def xRead(self, amount, offset):
        return b"".join(self.read(amount, offset))

    def xFileControl(self, *args):
        return False

    def xCheckReservedLock(self):
        return False

    def xLock(self, level):
        pass

    def xUnlock(self, level):
        pass

    def xClose(self):
        pass

    def xFileSize(self):
        return sum(o.size for o in self.bucket.objects.filter(Prefix=self.key + "/"))

    def xSync(self, flags):
        return True

    def xTruncate(self, newsize):
        return True

    def xWrite(self, data, offset):
        for block, start, write in self.blocks(offset, len(data)):
            assert write <= len(data)

            full_data = self.block(block)
            new_data = b"".join([
                full_data[0:start],
                data,
                full_data[start+write:],
            ])
            assert len(new_data) == BLOCK_SIZE

            self.block_object(block).put(
                Body=new_data,
            )
