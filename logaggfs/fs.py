import os
import threading
from hashlib import md5
import time

from basescript import init_logger
from deeputil import Dummy, generate_random_string

import fuse
from fuse import Fuse

from .mirrorfs import MirrorFS, MirrorFSFile, logit
from .mirrorfs import flag2mode

DUMMY_LOG = Dummy()

class LogaggFS(MirrorFS):
    pass

class LogaggFSFile(MirrorFSFile):

    @logit
    def write(self, buf, offset):
        self.file.seek(offset)
        self.file.write(buf)
        self.cached_file.write(self.path, buf)
        return len(buf)

class RotatingFile:
    def __init__(self, directory, state_file,
        max_file_size=500*1000, log=DUMMY_LOG):
        self.directory, self.state_file = os.path.abspath(directory), state_file
        self.max_file_size = max_file_size
        self.log = log

        self.path_list = None
        self.timestamp = str(time.time()).split('.')[0]
        self.fh = None

    def _make_file_set():
        '''
        make a set of files from state file
        '''
        fh = open(self.state_file)
        path_list = fh.readlines()
        path_set = set(i.split('\n')[0] for i in path_list)
        return path_set

    def rotate(self, text):
        '''
        Rotate the file, if necessary
        '''
        if (os.stat(self.filename_template).st_size>self.max_file_size) and text.endswith("\n"):
            self.close()
            self.timestamp = str(time.time()).split('.')[0]
            self.open()

    def open(self):
        self.fh = open(self.filename_template, 'a')

    def write(self, fpath, text=""):
        self.fpath = fpath
        self.open()
        self.fh.write(text)
        self.fh.flush()
        self.rotate(text)

    def close(self):
        self.fh.close()

    @property
    def filename_template(self):
        fpath = self.fpath.encode("utf-8")
        hash_fpath = md5(fpath).hexdigest()
        r = generate_random_string(5).decode("utf-8")
        return self.directory + '/' + hash_fpath + '.' + self.timestamp + '.' + r

class LogaggFuseRunner:
    def __init__(self):
        self.opts = None
        self.args = None
        self.fuse_server = None
        self.log_cache_dir = None
        self.log = Dummy()

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
        p.add_option(mountopt="root", metavar="PATH",
                                 help="mountpoint")
        p.add_option('--log-cache-dir',
                                help='directory where the logs are stored')
        p.add_option('--log-level', type=str, default='DEBUG',
                                help='level of logger [DEBUG, INFO]')
        p.add_option('--log-file', type=str, default=None,
                                help='file path to store logs')

        server.parse(values=server, errex=1)
        self.opts, self.args = server.parser.parse_args()

        #initiating logger
        self.log = DUMMY_LOG
        if self.opts.log_file:
            self.log = init_logger(fpath=self.opts.log_file,
                    pre_hooks=[], post_hooks=[],
                    level=self.opts.log_level)

        ldir = os.path.abspath(self.opts.log_cache_dir)
        ldir = os.path.join(ldir, '')[:-1]
        self.log_cache_dir = ldir

        server.log_cache_dir = self.log_cache_dir
        LogaggFSFile.log_cache_dir = self.log_cache_dir

        server.log = self.log
        MirrorFSFile.log = self.log

        self.log.debug('starting_up')
        #FIXME: report bug of init_logger not working with fpath=None
        try:
            if server.fuse_args.mount_expected():
                os.chdir(server.log_cache_dir)
        except OSError:
            log.exception("can't enter root of underlying filesystem", file=sys.stderr)
            sys.exit(1)


        #mkdir logs directory for keeping cached logs
        log_dir = os.path.abspath(os.path.join(self.log_cache_dir, "logs"))
        if not os.path.isdir(log_dir):
            self.log.debug('making_cache_directory', d=log_dir)
            os.makedirs(log_dir)
        self.log_dir = log_dir

        #touch state.txt
        state_file = os.path.abspath(os.path.join(self.log_cache_dir, "state.txt"))
        if not os.path.exists(state_file):
            self.log.debug('making_state_file', f=state_file)
            open(state_file, 'a').close()
        self.state_file = state_file

        LogaggFSFile.cached_file = RotatingFile(self.log_dir, state_file=self.state_file, log=self.log)

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
