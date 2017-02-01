"""Class PeakSO."""
from ..meta_base import MetaBase
from sql_scripts import peak_so_script as pss


class PeakSO(object, metaclass=MetaBase):
    """class PeakSO"""
    def __init__(self, pss_row):
        self.price_zone_code, self.hour_start, self.hour_end = pss_row
        self._init_on_load()

    def _init_on_load(self):
        """additional initialization"""
        key = (self.price_zone_code, self.hour_start)
        if key not in self.lst.keys():
            self.lst[key] = self

    def __repr__(self):
        return '<PeakSO pz:%i [%i - %i]>' % (self.price_zone_code, self.hour_start, self.hour_end)

    def get_peak_so_data(self):
        """get eq_db_peak_so view data"""
        return (self.price_zone_code, self.hour_start, self.hour_end)
