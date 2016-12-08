from sqlalchemy import *
from sqlalchemy.orm import relationship, reconstructor
from utils.ORM import Base, MetaBase

from sql_scripts import bid_init_script as bis
from sql_scripts import bid_hour_script as bhs
from sql_scripts import bid_pair_script as bps

from .bids_hour_data import BidHour


class Bid(Base, metaclass=MetaBase):
    __tablename__ = 'bids'
    dpg_id = Column(Integer)
    dpg_code = Column(String(16))
    bid_id = Column(Integer, primary_key=True)

    hour_data = relationship('BidHour', order_by='BidHour.hour')

    def __init__(self, bis_row):
        self.dpg_code = bis_row[bis['dpg_code']]
        self.dpg_id = bis_row[bis['dpg_id']]
        self.bid_id = bis_row[bis['bid_id']]
        self.hours = {}
        self.hours_index = {}
        self._init_on_load()

    lst = {'dpg_id': {}, 'dpg_code': {}}
    @reconstructor
    def _init_on_load(self):
        if self.dpg_id not in self.lst['dpg_id'].keys():
            self.lst['dpg_id'][self.dpg_id] = self
        if self.dpg_code not in self.lst['dpg_code'].keys():
            self.lst['dpg_code'][self.dpg_code] = self

    def __getitem__(self, item):
        if item in self.hours_index.keys():
            return self.hours[self.hours_index[item]]
        else:
            return None

    def __iter__(self):
        for n in sorted(self.hours.keys()):
            yield self.hours[n]

    def add_hour_data(self, bhs_row):
        hour = bhs_row[bhs['hour']]
        bid_hour_id = bhs_row[bhs['bid_hour_id']]
        self.hours_index[hour] = bid_hour_id
        if bid_hour_id in self.hours.keys():
            raise Exception('tried to add same hour to bid %s!' % self.dpg_code)
        self.hours[bid_hour_id] = BidHour(bhs_row)


    def add_intervals_data(self, bps_row):
        bid_hour_id = bps_row[bps['bid_hour_id']]
        self.hours[bid_hour_id].add_interval(bps_row)

    def serialize(self, session):
        session.add(self)
        session.flush()
        session.add_all(self.hours.values())
        session.flush()
        for hd in self.hours.values():
            session.add_all(hd.intervals.values())
