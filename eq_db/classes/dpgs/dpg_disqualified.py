from sqlalchemy import *

from utils.ORM import Base
from sql_scripts import disqualified_data_script as dds


class DpgDisqualified(Base):
    __tablename__ = 'dpgs_disqualified'
    dpg_id = Column(Integer, ForeignKey('dpg_demands.id'), primary_key=True)
    fed_station_cons = Column(Numeric)
    attached_supplies_gen = Column(Numeric)

    def __init__(self, dds_row):
        self.dpg_id = dds_row[dds['dpg_id']]
        self.fed_station_cons = dds_row[dds['fed_station_cons']]
        self.attached_supplies_gen = dds_row[dds['attached_supplies_gen']]

    def serialize(self, session):
        session.add(self)
