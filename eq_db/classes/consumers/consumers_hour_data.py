from sqlalchemy import *
from sqlalchemy.orm import relationship

from utils.ORM import Base
from sql_scripts import rastr_consumer_script as rc


class ConsumerHourData(Base):
    __tablename__ = 'consumers_hour_data'
    consumer_code = Column(Integer, ForeignKey('consumers.code'), primary_key=True)
    hour = Column(Integer, primary_key=True)
    type = Column(Integer)
    pdem = Column(Numeric)

    def __init__(self, rc_row):
        self.consumer_code = rc_row[rc['consumer_code']]
        self.hour = rc_row[rc['hour']]
        self.type = rc_row[rc['type']]  # 1 - система, 0 - нагрузка
        self.pdem = rc_row[rc['pdem']]
