from sqlalchemy import *
from sqlalchemy.orm import relationship

from utils.ORM import Base
from sql_scripts import bid_hour_script as bhs
from sql_scripts import bid_pair_script as bps

from .bids_hour_interval_data import BidHourInterval


class BidHour(Base):
    __tablename__ = 'bids_hour_data'
    bid_id = Column(Integer, ForeignKey('bids.bid_id'), primary_key=True)
    bid_hour_id = Column(Integer, unique=True)
    hour = Column(Integer, primary_key=True)

    interval_data = relationship('BidHourInterval', order_by='BidHourInterval.interval_number')
    max_price = relationship('BidMaxPrice', uselist=False, primaryjoin='BidMaxPrice.hour == BidHour.hour', foreign_keys='BidMaxPrice.hour')

    def __init__(self, bhs_row):
        self.bid_id = bhs_row[bhs['bid_id']]
        self.bid_hour_id = bhs_row[bhs['bid_hour_id']]
        self.hour = bhs_row[bhs['hour']]
        self.intervals = {}

    def add_interval(self, bps_row):
        interval_number = bps_row[bps['interval_number']]
        if interval_number in self.intervals.keys():
            raise Exception('tried to add same interval twice!')
        self.intervals[interval_number] = BidHourInterval(bps_row)
