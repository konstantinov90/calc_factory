from sqlalchemy import Column, Integer, Numeric, ForeignKey
from utils.ORM import Base

from sql_scripts import generators_last_hour_script as glhs


class DguLastHour(Base):
    __tablename__ = 'dgus_last_hour'
    dgu_code = Column(Integer, ForeignKey('dgus.code'), primary_key=True)
    volume = Column(Numeric)

    def __init__(self, glhs_row):
        self.dgu_code = glhs_row[glhs['rge_code']]
        self.volume = glhs_row[glhs['volume']]

    def get_data(self):
        return (self.dgu_code, self.volume)
