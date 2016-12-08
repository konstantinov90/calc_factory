from sqlalchemy import *

from utils.ORM import Base


class BidMaxPrice(Base):
    __tablename__ = 'bids_max_price'
    hour = Column(Integer, primary_key=True)
    price = Column(Numeric)

    def __init__(self, hour, max_price):
        self.hour = hour
        self.price = max_price
