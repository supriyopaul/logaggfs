import os
import threading
from hashlib import md5
import time

from basescript import init_logger
from deeputil import Dummy, generate_random_string
from deeputil import keeprunning

import fuse
from fuse import Fuse

from .mirrorfs import MirrorFS, MirrorFSFile, logit
from .mirrorfs import flag2mode

DUMMY_LOG = Dummy()

class TrackList:

    REFRESH_INTERVAL = 0.5

    def __init__(self, state_file, directory, log=DUMMY_LOG):
        self.state_file = state_file
        self.directory = directory
        self.path_set = set()
        self.log = log

        # launch refresh thread
        th = threading.Thread(target=self._update_file_set)
        th.daemon = True
        th.start()
        self.update_file_set = th

    @keeprunning(wait_secs=REFRESH_INTERVAL)
    def _update_file_set(self):
        '''
        make a set of files from state file
        '''
        self.log.debug('checking_state_file_for_changes', tracked_files=self.path_set)
        fh = open(self.state_file)
        path_list = fh.readlines()
        path_set = set(i.split('\n')[0] for i in path_list)
        fh.close()
        self.path_set = path_set

        time.sleep(self.REFRESH_INTERVAL)

class LogaggFS(MirrorFS):
    pass

class LogaggFSFile(MirrorFSFile):

    @logit
    # FIXME: take path also as parameter
    def __init__(self, *args, **kwargs):
        super().__init__( *args, **kwargs)

        if self.frompath in self.tracklist.path_set:
            self.capture = True
            self.rfile = RotatingFile(
                    self.tracklist.directory,
                    self._compute_hash(self.frompath))
        else:
            self.capture = False

    def _compute_hash(self, fpath):
        fpath = fpath.encode("utf-8")
        hash_fpath = md5(fpath).hexdigest()
        return(hash_fpath)


    @logit
    def write(self, buf, offset):
        self.file.seek(offset)
        self.file.write(buf)
        if self.capture:
            self.log.debug('writing_to_rotating_file', file=self.rfile)
            self.rfile.write(buf)
        return len(buf)

class RotatingFile:
    def __init__(self, directory, filename,
        max_file_size=500*1000, log=DUMMY_LOG):

        self.directory, self.filename = os.path.abspath(directory), filename
        self.max_file_size = max_file_size
        self.log = log

        self.timestamp = str(time.time())
        self.fh = None
        self._open()

    def _rotate(self, text):
        '''
        Rotate the file, if necessary
        '''
        if (os.stat(self.filename_template).st_size>self.max_file_size) and text.endswith("\n"):
            self._close()
            self.timestamp = str(time.time())
            self._open()

    def _open(self):
        self.fh = open(self.filename_template, 'a')

    def write(self, text=""):
        self._open()
        self.fh.write(text)
        self.fh.flush()
        self._rotate(text)

    def _close(self):
        self.fh.close()

    @property
    def filename_template(self):
        r = generate_random_string(5).decode("utf-8")
        return self.directory + '/' + self.filename + '.' + self.timestamp


class LogaggFuseRunner:
    def __init__(self):
        self.opts = None
        self.args = None
        self.fuse_server = None
        self.log_cache_dir = None
        self.state_file = None
        self.log_cache_dir = None
        self.runfs_thread = None
        self.log = Dummy()

    def _mkdir_logdir(self, parent_directory):

        log_dir = os.path.abspath(os.path.join(parent_directory,
                                "logs"))
        if not os.path.isdir(log_dir):
            self.log.debug('making_cache_directory', d=log_dir)
            os.makedirs(log_dir)
        return log_dir

    def _touch_statefile(self, parent_directory):

        state_file = os.path.abspath(os.path.join(parent_directory,
                                "trackfiles.txt"))
        if not os.path.exists(state_file):
            self.log.debug('making_state_file', f=state_file)
            open(state_file, 'a').close()
        return state_file

    def runfs(self):
        usage = """
    Logagg Log collection FUSE filesystem

    """ + Fuse.fusage
        #argument parsing
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
            log.exception("cannot_enter_root_of_underlying_filesystem", file=sys.stderr)
            sys.exit(1)

        # mkdir logs directory and state file inside log cache directory
        self.log_dir = self._mkdir_logdir(parent_directory=self.log_cache_dir)
        self.state_file = self._touch_statefile(parent_directory=self.log_cache_dir)

        # create tracklist for monitoring log files
        tracklist = TrackList(state_file=self.state_file,
                        directory=self.log_dir,
                        log=self.log)
        LogaggFSFile.tracklist = tracklist

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
