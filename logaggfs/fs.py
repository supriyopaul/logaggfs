#!/usr/bin/env python

import os
import threading
from hashlib import md5
import time
import glob

from basescript import init_logger
from deeputil import Dummy
from deeputil import AttrDict

import fuse
from fuse import Fuse

from .mirrorfs import MirrorFS, MirrorFSFile, logit
from .mirrorfs import flag2mode

DUMMY_LOG = Dummy()

class TrackList:

    def __init__(self, state_file, directory, log=DUMMY_LOG):
        self.state_file = state_file
        self.directory = directory
        self.log = log

        self.fpaths = dict()

    def update(self):
        '''
        make a set of files from state file
        '''

        fh = open(self.state_file)
        path_list = fh.readlines()
        fpaths = dict()

        # Make an empty like dictionary of new fpaths
        for pattern in path_list:
            for fpath in glob.glob(pattern[:-1]):
                fpaths[fpath] = None

        # Chen new fpaths to ones ones and modify
        for path in fpaths:

            if path not in self.fpaths: self.fpaths[path] = fpaths[path]

        for path in self.fpaths:

            if path not in fpaths: self.fpaths.pop(path)

        self.log.debug('checking_state_file_for_changes', tracked_files=self.fpaths)
        fh.close()


class LogaggFS(MirrorFS):
    pass

class LogaggFSFile(MirrorFSFile):


    @logit
    # FIXME: take path also as parameter
    def __init__(self, *args, **kwargs):
        super().__init__( *args, **kwargs)

        if self.frompath == "/.update":
            self.tracklist.update()

        self.full_path = self.mountpoint + self.frompath

        if self.full_path in self.tracklist.fpaths and self.tracklist.fpaths[self.full_path] is None:
            #import pdb; pdb.set_trace()
            self.tracklist.fpaths[self.full_path] = RotatingFile(
                                                        self.tracklist.directory,
                                                        self._compute_hash(self.full_path)
                                                        )

    def _compute_hash(self, fpath):
        fpath = fpath.encode("utf-8")
        hash_fpath = md5(fpath).hexdigest()
        return(hash_fpath)


    @logit
    def write(self, buf, offset):
        self.file.seek(offset)
        self.file.write(buf)
        if self.tracklist.fpaths.get(self.full_path):
            self.log.debug('writing_to_rotating_file', file=self.tracklist.fpaths[self.full_path])
            self.tracklist.fpaths[self.full_path].write(buf)
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
        if (os.stat(self.filename_template).st_size > self.max_file_size) and text.endswith("\n"):
            self._close()
            self.timestamp = str(time.time())
            self._open()

    def _open(self):
        self.log.debug('new_rotating_file_created', f=self.filename_template)
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
        return self.directory + '/' + self.filename + '.' + self.timestamp + '.log'


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
        p.add_option(mountopt='root', metavar='PATH',
                                 help='mountpoint')
        p.add_option(mountopt='loglevel', metavar='DEBUG/INFO' ,default='INFO',
                                help='level of logger')
        p.add_option(mountopt='logfile', metavar='PATH', default='/tmp/fuse.log',
                                help='file path to store logs')

        server.parse(values=server, errex=1)
        self.opts, self.args = server.parser.parse_args()

        #initiating logger
        self.log = DUMMY_LOG
        if self.opts.logfile:
            self.log = init_logger(fpath=self.opts.logfile,
                                level=self.opts.loglevel)

        ldir = os.path.abspath(server.root)
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
        LogaggFSFile.mountpoint = server.fuse_args.mountpoint

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
