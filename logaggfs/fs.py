import os
import threading

from basescript import init_logger
from deeputil import Dummy
import fuse
from fuse import Fuse

from .mirrorfs import MirrorFS, MirrorFSFile, logit

class LogaggFS(MirrorFS):
    pass

class LogaggFSFile(MirrorFSFile):
   @logit
   def write(self, buf, offset):
        import pdb; pdb.set_trace()
        self.file.seek(offset)
        self.file.write(buf)
        self.cache.write(buf)
        return len(buf)

class RotatingFile:
    def __init__(self, directory, filename, max_files=100,
        max_file_size=500*1000, log=Dummy):
        self.ii = 1
        self.directory, self.filename      = os.path.abspath(directory), filename
        self.max_file_size, self.max_files = max_file_size, max_files
        self.finished, self.fh             = False, None
        self.open()

    def rotate(self, text):
        """Rotate the file, if necessary"""
        if (os.stat(self.filename_template).st_size>self.max_file_size) and text.endswith("\n"):
            self.close()
            self.ii += 1
            if (self.ii<=self.max_files):
                self.open()
            else:
                self.close()
                self.finished = True

    def open(self):
        self.fh = open(self.filename_template, 'w')

    def write(self, text=""):
        self.fh.write(text)
        self.fh.flush()
        self.rotate(text)

    def close(self):
        self.fh.close()

    @property
    def filename_template(self):
        return self.directory + '/' + self.filename + "%0.2d" % self.ii

class LogaggFuseRunner:
    def __init__(self):
        self.opts = None
        self.args = None
        self.fuse_server = None
        self.log = Dummy()

    def _parse_root_dir(self, directory):
        if os.path.isdir(os.path.abspath(directory)):
            return os.path.join(os.path.abspath(directory))
        elif not os.path.exists(directory):
            os.makedirs(directory)
            return os.path.join(os.path.abspath(directory))


    def runfs(self):
        usage = """
    Logagg Log collection FUSE filesystem

    """ + Fuse.fusage

        server = LogaggFS(version="%prog " + fuse.__version__,
                     usage=usage,
                     dash_s_do='setsingle',
                     file_class=LogaggFSFile)
        self.fuse_server = server

        p = server.parser
        p.add_option(mountopt="root", metavar="PATH", default='/logfs/',
                                 help="mount to this directory[default: %default]")
        p.add_option('--mount-from', default='/logfs/cache/mirror/',
                                help='directory to be mounted to mirrorfs, [default: %default]')
        p.add_option('--log-level', type=str, default='DEBUG',
                                help='level of logger [DEBUG, INFO]')
        p.add_option('--log-file', type=str, default=None,
                                help='file path to store logs')

        server.parse(values=server, errex=1)
        self.opts, self.args = server.parser.parse_args()
        #initiating logger
        if self.opts.log_file:
            self.log = init_logger(fpath=self.opts.log_file,
                    pre_hooks=[], post_hooks=[],
                    level=self.opts.log_level)


        import pdb; pdb.set_trace()
        # mkdir root path if not there
        root_dir = os.path.abspath(os.path.join(self.opts.root, "mountpoint")
        if not os.path.isdir(root_dir):
            self.log.debug('making_root_directory', d=root_dir)
            os.makedirs(root_dir)
        server.root = root_dir

        # mkdir directory from which we are mounting from
        mount_from = os.path.abspath(self.opts.mount_from)
        if not os.path.isdir(mount_from):
            self.log.debug('making_from_directory', d=mount_from)
            os.makedirs(mount_from)

        LogaggFSFile.root = server.root

        self.log.debug('starting_up')
        #FIXME: report bug of init_logger not working with fpath=None
        try:
            if server.fuse_args.mount_expected():
                os.chdir(server.root)
        except OSError:
            log.exception("can't enter root of underlying filesystem", file=sys.stderr)
            sys.exit(1)

        server.log = self.log
        MirrorFSFile.log = self.log

        #mkdir logs directory for keeping cached logs
        cache_dir = os.path.abspath(os.path.join(self.opts.root, "logs"))
        if not os.path.isdir(cache_dir):
            self.log.debug('making_cache_directory', d=cache_dir)
            os.makedirs(cache_dir)
        self.cache_dir = cache_dir

        LogaggFSFile.cache = RotatingFile(self.cache_dir, state_file=self.state_file log=self.log)

        #touch monitered-files.txt
        state_file = os.path.abspath(os.path.join(self.opts.root, "monitered-files.txt"))
        if not os.path.exits(state_file)
        server.main()


    def start(self):
        th = threading.Thread(target=self.runfs)
        th.daemon = True
        th.start()
        self.runfs_thread = th
        th.join()


def main():
    runner = LogaggFuseRunner()
    runner.start()
