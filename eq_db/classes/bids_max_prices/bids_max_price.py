"""Class BidMaxPrice."""
from ..meta_base import MetaBase


class BidMaxPrice(object, metaclass=MetaBase):
    """class BidMaxPrice"""
    def __init__(self, hour, max_price):
        self.hour = hour
        self.price = max_price
        self._init_on_load()

    def _init_on_load(self):
        """additional initialization"""
        if self.hour not in self.lst.keys():
            self.lst[self.hour] = self

    def __repr__(self):
        return '<BidMaxPrice %i: %f>' % (self.hour, self.price)
