from sqlalchemy import *

from utils.ORM import Base
from sql_scripts import rastr_areas_script as ra


class AreaHourData(Base):
    __tablename__ = 'areas_hour_data'
    area_code = Column(Integer, ForeignKey('areas.code'), primary_key=True)
    hour = Column(Integer, primary_key=True)
    losses = Column(Numeric)
    load_losses = Column(Numeric)
    sum_pn_retail_diff = Column(Numeric)
    nodes_on = Column(Integer)

    def __init__(self, ra_row):
        self.area_code = ra_row[ra['area']]
        self.hour = ra_row[ra['hour']]
        self.losses = ra_row[ra['losses']]
        self.load_losses = ra_row[ra['load_losses']]
        self.sum_pn_retail_diff = None
        self.nodes_on = None
