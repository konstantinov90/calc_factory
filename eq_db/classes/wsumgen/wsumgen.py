"""Class Wsumgen."""
import itertools
from ..meta_base import MetaBase


class Wsumgen(object, metaclass=MetaBase):
    """class Wsumgen"""
    seq = itertools.count()

    def __init__(self, ws_row):
        self._id = next(self.seq)
        self.dgu_code, self.price, self.integral_id, self.hour_start, \
            self.hour_end, self.min_volume, self.volume = ws_row
        self._init_on_load()

    def _init_on_load(self):
        """additional initialization"""
        if self._id not in self.lst.keys():
            self.lst[self._id] = self

    def get_fuel_data(self):
        """get eq_db_fuel view data"""
        return (self.integral_id, self.dgu_code, self.min_volume,
                self.volume, self.hour_start, self.hour_end)
