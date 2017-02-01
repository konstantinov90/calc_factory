"""Class PriceZone."""
import itertools
from operator import attrgetter
from ..meta_base import MetaBase
from ..dpgs.dpg_demand import DpgDemand
from .price_zone_hour_data import PriceZoneHourData


class PriceZone(object, metaclass=MetaBase):
    """"class PriceZone"""
    def __init__(self, pzs_row):
        self.code, self.power_consumption = pzs_row
        self._hour_data = {}
        self._init_on_load()

    def __repr__(self):
        return '<PriceZone %i>' % self.code

    @property
    def hour_data(self):
        """hour_data property"""
        return sorted(self._hour_data.values(), key=attrgetter('hour'))

    def _init_on_load(self):
        """additional initialization"""
        if self.code not in self.lst.keys():
            self.lst[self.code] = self

    def add_price_zone_hour_data(self, hour):
        """add PriceZoneHourData instance"""
        self._hour_data[hour] = PriceZoneHourData(hour, self.code)

    def get_consumption(self):
        """get sum consumption for PriceZone"""
        return [pzh.get_consumption() for pzh in self.hour_data]

    def get_settings(self):
        """get eq_db_price_zone_settings view data"""
        volume = sum(dpg.dem_rep_volume if dpg.consumer.dem_rep_ready else 0 for dpg in DpgDemand
                     if not dpg.is_unpriced_zone and not dpg.supply_gaes and not dpg.is_fsk and dpg.price_zone_code == self.code)
        pivot = 0.002 * self.power_consumption

        ratio = 0.01 * (volume / pivot if volume < pivot else 1)
        mode = 1 if volume else 0
        return (self.code, mode, ratio)
