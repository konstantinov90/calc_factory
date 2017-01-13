"""redefined print function to print within update bar"""
import sys


def print(text):
    """redefine built-in print"""
    sys.stdout.write('\r%s\n' % text)
    sys.stdout.flush()
