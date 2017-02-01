"""Class Wsumgen."""
import itertools
from ..meta_base import MetaBase


class Wsumgen(object, metaclass=MetaBase):
    """class Wsumgen"""
    seq = itertools.count()

    def __init__(self, ws_row):
        self._id = next(self.seq)
        self.dgu_code, self.price, self.integral_id, self.hour_start, \
            self.hour_end, self.min_volume, self.volume, self.group_code = ws_row
        self.dgu = None
        self._init_on_load()

    def _init_on_load(self):
        """additional initialization"""
        if self._id not in self.lst.keys():
            self.lst[self._id] = self

    def set_dgu(self, dgu):
        """add Dgu instance"""
        self.dgu = dgu

    def get_fuel_data(self):
        """get eq_db_fuel view data"""
        return (self.integral_id, self.dgu_code, self.min_volume,
                self.volume, self.hour_start, self.hour_end)

    def recalculate(self):
        """additional calculation"""
        dgus = (ws.dgu for ws in Wsumgen if ws.group_code == self.group_code)
        hours = range(self.hour_start, self.hour_end + 1)
        self.volume = sum(_hd.p for dgu in dgus for _hd in dgu.hour_data if _hd.hour in hours)
