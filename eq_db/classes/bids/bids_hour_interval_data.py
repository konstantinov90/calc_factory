from sqlalchemy import *

from utils.ORM import Base
from sql_scripts import bid_pair_script as bps


class BidHourInterval(Base):
    __tablename__ = 'bids_hour_interval_data'
    bid_hour_id = Column(Integer, ForeignKey('bids_hour_data.bid_hour_id'), primary_key=True)
    interval_number = Column(Integer, primary_key=True)
    price = Column(Numeric)
    volume = Column(Numeric)
    volume_init = Column(Numeric)

    def __init__(self, bps_row):
        self.bid_hour_id = bps_row[bps['bid_hour_id']]
        self.interval_number = bps_row[bps['interval_number']]
        self.price = bps_row[bps['price']]
        self.volume = bps_row[bps['volume']]
        self.volume_init = bps_row[bps['volume_init']]
