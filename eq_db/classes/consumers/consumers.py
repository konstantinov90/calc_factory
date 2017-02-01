"""Class Consumer."""
from operator import attrgetter
from ..meta_base import MetaBase
from .consumers_hour_data import ConsumerHourData


class Consumer(object, metaclass=MetaBase):
    """class Consumer"""
    def __init__(self, rc_row):
        self.code = rc_row.consumer_code
        self.dem_rep_ready = bool(rc_row.dem_rep_ready)
        self.consumer_hour_data = {}
        self._init_on_load()

    @property
    def hour_data(self):
        """hour_data property"""
        return sorted(self.consumer_hour_data.values(), key=attrgetter('hour'))

    def _init_on_load(self):
        """additional initialization"""
        if self.code not in self.lst.keys():
            self.lst[self.code] = self

    def __repr__(self):
        return '<Consumer %i>' % self.code

    def add_consumer_hour_data(self, rc_row):
        """add ConsumerHourData instance"""
        hour = rc_row.hour
        if hour in self.consumer_hour_data.keys():
            raise Exception('tried to add consumer %i hour twice' % self.code)
        self.consumer_hour_data[hour] = ConsumerHourData(rc_row)
