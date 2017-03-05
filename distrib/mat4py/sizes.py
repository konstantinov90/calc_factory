"""Artificial class for size determination"""


class ZeroSize(object):
    """class for zero length matrix size"""
    def __init__(self, col_num):
        self.col_num = col_num

    def __len__(self):
        return 1
