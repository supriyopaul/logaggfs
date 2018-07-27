import os, sys
from errno import *
from stat import *
import fcntl

try:
    import _find_fuse_parts
except ImportError:
    pass
import fuse
from fuse import Fuse

from deeputil import Dummy

DUMMY_LOG = Dummy()

if not hasattr(fuse, '__version__'):
    raise RuntimeError("your fuse-py doesn't know of fuse.__version__, probably it's too old.")

fuse.fuse_python_api = (0, 2)

fuse.feature_assert('stateful_files', 'has_init')

def flag2mode(flags):
    md = {os.O_RDONLY: 'r', os.O_WRONLY: 'w', os.O_RDWR: 'w+'}
    m = md[flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]

    if flags | os.O_APPEND:
        m = m.replace('w', 'a', 1)

    return m

def logit(fn):
    def _fn(*args, **kwargs):
        self = args[0]
        fnname = fn.__name__
        self.log.debug(fnname, args=args, kwargs=kwargs)
        try:
            r = fn(*args, **kwargs)
        except Exception:
            self.log.exception('logit_exception_{}'.format(fnname))
            raise
        return r

    return _fn

class MirrorFSFile(object):

    @logit
    def __init__(self, path, flags, *mode):
        self.frompath = path
        self.path = path = self.log_cache_dir + '/mirror' + path
        self.file = os.fdopen(os.open(path, flags, *mode),
                              flag2mode(flags))
        self.fd = self.file.fileno()

    @logit
    def read(self, length, offset):
        self.file.seek(offset)
        return self.file.read(length)

    @logit
    def write(self, buf, offset):
        self.file.seek(offset)
        self.file.write(buf)
        return len(buf)

    @logit
    def release(self, flags):
        self.file.close()

    def _fflush(self):
        if 'w' in self.file.mode or 'a' in self.file.mode:
            self.file.flush()

    @logit
    def fsync(self, isfsyncfile):
        self._fflush()
        if isfsyncfile and hasattr(os, 'fdatasync'):
            os.fdatasync(self.fd)
        else:
            os.fsync(self.fd)

    @logit
    def flush(self):
        self._fflush()
        # cf. xmp_flush() in fusexmp_fh.c
        os.close(os.dup(self.fd))

    @logit
    def fgetattr(self):
        return os.fstat(self.fd)

    @logit
    def ftruncate(self, len):
        self.file.truncate(len)

    @logit
    def lock(self, cmd, owner, **kw):
        # The code here is much rather just a demonstration of the locking
        # API than something which actually was seen to be useful.

        # Advisory file locking is pretty messy in Unix, and the Python
        # interface to this doesn't make it better.
        # We can't do fcntl(2)/F_GETLK from Python in a platfrom independent
        # way. The following implementation *might* work under Linux.
        #
        # if cmd == fcntl.F_GETLK:
        #     import struct
        #
        #     lockdata = struct.pack('hhQQi', kw['l_type'], os.SEEK_SET,
        #                            kw['l_start'], kw['l_len'], kw['l_pid'])
        #     ld2 = fcntl.fcntl(self.fd, fcntl.F_GETLK, lockdata)
        #     flockfields = ('l_type', 'l_whence', 'l_start', 'l_len', 'l_pid')
        #     uld2 = struct.unpack('hhQQi', ld2)
        #     res = {}
        #     for i in xrange(len(uld2)):
        #          res[flockfields[i]] = uld2[i]
        #
        #     return fuse.Flock(**res)

        # Convert fcntl-ish lock parameters to Python's weird
        # lockf(3)/flock(2) medley locking API...
        op = { fcntl.F_UNLCK : fcntl.LOCK_UN,
               fcntl.F_RDLCK : fcntl.LOCK_SH,
               fcntl.F_WRLCK : fcntl.LOCK_EX }[kw['l_type']]
        if cmd == fcntl.F_GETLK:
            return -EOPNOTSUPP
        elif cmd == fcntl.F_SETLK:
            if op != fcntl.LOCK_UN:
                op |= fcntl.LOCK_NB
        elif cmd == fcntl.F_SETLKW:
            pass
        else:
            return -EINVAL

        fcntl.lockf(self.fd, op, kw['l_start'], kw['l_len'])

class MirrorFS(Fuse):

    def __init__(self,
            file_class=None,
            *args,
            **kw):
        Fuse.__init__(self, *args, **kw)

        self.log = DUMMY_LOG
        self._log_cache_dir = None
        self._mirror_dir = None
        self.file_class = file_class or MirrorFSFile

    @property
    def log_cache_dir(self):
        return self._log_cache_dir

    @log_cache_dir.setter
    def log_cache_dir(self, v):
        self._log_cache_dir = v
        self._mirror_dir = v + '/mirror'
        if not os.path.exists(self._mirror_dir):
            os.makedirs(self._mirror_dir)

    def _mappath(self, path):
        _path = self._mirror_dir + path
        self.log.debug('_mappath', fromp=path, top=_path,
                log_cache_dir=self._log_cache_dir)
        return _path

    @logit
    def getattr(self, path):
        path = self._mappath(path)
        return os.lstat(path)

    @logit
    def readlink(self, path):
        path = self._mappath(path)
        return os.readlink(path)

    @logit
    def readdir(self, path, offset):
        path = self._mappath(path)
        self.log.debug('readdir', path=path, offset=offset)
        for e in os.listdir(path):
            yield fuse.Direntry(e)

    @logit
    def unlink(self, path):
        path = self._mappath(path)
        os.unlink(path)

    @logit
    def rmdir(self, path):
        path = self._mappath(path)
        os.rmdir(path)

    @logit
    def symlink(self, path, path1):
        path = self._mappath(path)
        path1 = self._mappath(path1)
        os.symlink(path, path1)

    @logit
    def rename(self, path, path1):
        path = self._mappath(path)
        path1 = self._mappath(path1)
        os.rename(path, path1)

    @logit
    def link(self, path, path1):
        path = self._mappath(path)
        path1 = self._mappath(path1)
        os.link(path, path1)

    @logit
    def chmod(self, path, mode):
        path = self._mappath(path)
        os.chmod(path, mode)

    @logit
    def chown(self, path, user, group):
        path = self._mappath(path)
        os.chown(path, user, group)

    @logit
    def truncate(self, path, len):
        path = self._mappath(path)
        f = open(path, "a")
        f.truncate(len)
        f.close()

    @logit
    def mknod(self, path, mode, dev):
        path = self._mappath(path)
        os.mknod(path, mode, dev)

    @logit
    def mkdir(self, path, mode):
        path = self._mappath(path)
        os.mkdir(path, mode)

    @logit
    def utime(self, path, times):
        path = self._mappath(path)
        os.utime(path, times)

#    The following utimens method would do the same as the above utime method.
#    We can't make it better though as the Python stdlib doesn't know of
#    subsecond preciseness in acces/modify times.
#
#    def utimens(self, path, ts_acc, ts_mod):
#      os.utime(path, (ts_acc.tv_sec, ts_mod.tv_sec))

    @logit
    def access(self, path, mode):
        path = self._mappath(path)
        if not os.access(path, mode):
            return -EACCES

#    This is how we could add stub extended attribute handlers...
#    (We can't have ones which aptly delegate requests to the underlying fs
#    because Python lacks a standard xattr interface.)
#
#    def getxattr(self, path, name, size):
#        val = name.swapcase() + '@' + path
#        if size == 0:
#            # We are asked for size of the value.
#            return len(val)
#        return val
#
#    def listxattr(self, path, size):
#        # We use the "user" namespace to please XFS utils
#        aa = ["user." + a for a in ("foo", "bar")]
#        if size == 0:
#            # We are asked for size of the attr list, ie. joint size of attrs
#            # plus null separators.
#            return len("".join(aa)) + len(aa)
#        return aa

    @logit
    def statfs(self):
        """
        Should return an object with statvfs attributes (f_bsize, f_frsize...).
        Eg., the return value of os.statvfs() is such a thing (since py 2.2).
        If you are not reusing an existing statvfs object, start with
        fuse.StatVFS(), and define the attributes.

        To provide usable information (ie., you want sensible df(1)
        output, you are suggested to specify the following attributes:

            - f_bsize - preferred size of file blocks, in bytes
            - f_frsize - fundamental size of file blcoks, in bytes
                [if you have no idea, use the same as blocksize]
            - f_blocks - total number of blocks in the filesystem
            - f_bfree - number of free blocks
            - f_files - total number of file inodes
            - f_ffree - nunber of free file inodes
        """

        return os.statvfs(self._mirror_dir)

    @logit
    def fsinit(self):
        os.chdir(self._mirror_dir)

    def main(self, *a, **kw):
        return Fuse.main(self, *a, **kw)
