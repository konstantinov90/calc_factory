from sqlalchemy import *
from sqlalchemy.orm import reconstructor

from utils.ORM import Base, MetaBase
from sql_scripts import wsumgen_script as ws



class Wsumgen(Base, metaclass=MetaBase):
    __tablename__ = 'wsumgen'
    id = Column(Integer, primary_key=True)
    dgu_code = Column(Integer, ForeignKey('dgus.code'), unique=True)
    price = Column(Numeric)
    integral_id = Column(Integer)
    hour_start = Column(Integer)
    hour_end = Column(Integer)
    min_volume = Column(Numeric)
    volume = Column(Numeric)

    def __init__(self, ws_row):
        self.dgu_code = ws_row[ws['rge_code']]
        self.price = ws_row[ws['price']]
        self.integral_id = ws_row[ws['integral_id']]
        self.hour_start = ws_row[ws['hour_start']]
        self.hour_end = ws_row[ws['hour_end']]
        self.min_volume = ws_row[ws['min_volume']]
        self.volume = ws_row[ws['volume']]

    @reconstructor
    def _init_on_load(self):
        if self.id not in self.lst.keys():
            self.lst[self.id] = self

    def get_fuel_data(self):
        return (self.integral_id, self.dgu_code, self.min_volume, 
                self.volume, self.hour_start, self.hour_end)

    def serialize(self, session):
        session.add(self)
        session.flush()
