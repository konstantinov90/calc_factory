import sys


# Accepts a float between 0 and 1. Any int will be converted to a float.
# A value under 0 represents a 'halt'.
# A value at 1 or bigger represents 100%
def update_progress(progress):
    """brick progress bar"""
    bar_length = 15  # Modify this to change the length of the progress bar
    status = ""
    if isinstance(progress, int):
        progress = float(progress)
    if not isinstance(progress, float):
        progress = 0
        status = "error: progress var must be float\r\n"
    if progress < 0:
        progress = 0
        status = "Halt...\r\n"
    if progress >= 1:
        progress = 1
        status = "Done...\r\n"
    block = int(round(bar_length*progress))
    if progress >= 1:
        text = '\r%s\r' % (' ' * 50)
    else:
        text = "\rPercent: [{0}] {1}% {2}".format("#"*block + "-"*(bar_length-block),
                                                  round(progress*100), status)
    sys.stdout.write(text)
    sys.stdout.flush()


class UnlimBar(object):
    """unlimited progress bar"""
    def __init__(self):
        self.limit = 13
        self.val = 0
        self.frequency = 1 / 100
        self.direction = 1
        self.i = 0

    def update(self):
        """update progress"""
        if not self.i * self.frequency % 100:
            self.val += self.direction
            if self.val == self.limit:
                self.direction = -1
            if self.val == 0:
                self.direction = 1
            text = "\r..{0}".format("."*self.val)
            sys.stdout.write(text)
            sys.stdout.flush()
        self.i += 1

    @staticmethod
    def finish():
        """finish bar"""
        sys.stdout.write('\n\r')
        sys.stdout.flush()


class RollerBar(object):
    """roller progress bar"""
    def __init__(self):
        self.vals = ['| | |', '\\ / \\', '- - -', '/ \\ /']
        self.val = self.vals[0]
        self.ind = 0
        self.N = len(self.vals)
        self.period = 25
        self.frequency = 1 / self.period
        self.i = 0

    def roll(self):
        """update progress"""
        if not self.i * self.frequency % self.period:
            self.ind += 1
            self.val = self.vals[self.ind % self.N]
            text = "\r{0}".format(self.val)
            sys.stdout.write(text)
            sys.stdout.flush()
        self.i += 1

    @staticmethod
    def finish():
        """kill roller"""
        sys.stdout.write('\r')
        sys.stdout.flush()
