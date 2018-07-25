class RotatingFile:
    def __init__(self, directory, state_file, max_files=100,
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

dir = cache
state_file = f.txt

