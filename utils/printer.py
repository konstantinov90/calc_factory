"""redefined print function to print within update bar"""
import sys


def display(text):
    """redefine built-in print"""
    sys.stdout.write('\r%s\n' % str(text))
    sys.stdout.flush()
