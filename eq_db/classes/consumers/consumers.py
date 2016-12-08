from sqlalchemy import *
from sqlalchemy.orm import relationship, reconstructor

from utils.ORM import Base, MetaBase
from sql_scripts import rastr_consumer_script as rc
from .consumers_hour_data import ConsumerHourData


class Consumer(Base, metaclass=MetaBase):
    __tablename__ = 'consumers'
    code = Column(Integer, primary_key=True)

    hour_data = relationship('ConsumerHourData', order_by='ConsumerHourData.hour', lazy='subquery')

    def __init__(self, rc_row):
        self.code = rc_row[rc['consumer_code']]
        self.consumer_hour_data = {}

    @reconstructor
    def _init_on_load(self):
        if self.code not in self.lst.keys():
            self.lst[self.code] = self

    def add_consumer_hour_data(self, rc_row):
        hour = rc_row[rc['hour']]
        if hour in self.consumer_hour_data.keys():
            raise Exception('tried to add consumer %i hour twice' % self.code)
        self.consumer_hour_data[hour] = ConsumerHourData(rc_row)
