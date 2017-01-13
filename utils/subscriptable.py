"""subscriptable decorator"""


class Subscriptable(object):
    """Subscriptable dummy class"""
    def __init__(self, fnc):
        if hasattr(fnc, '__func__'):
            self.fun = fnc.__func__
        else:
            self.fun = fnc

    def __getitem__(self, *item):
        return self.fun(*item)

    def __call__(self, *args):
        return self.fun(*args)


def subscriptable(fun):
    """make function/method subscriptable"""
    return Subscriptable(fun)
