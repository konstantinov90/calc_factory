from sqlalchemy import *

from utils.ORM import Base
from sql_scripts import rastr_load_script as rl


class LoadHourData(Base):
    __tablename__ = 'loads_hour_data'
    consumer_code = Column(Integer, ForeignKey('loads.consumer_code'), primary_key=True)
    hour = Column(Integer, primary_key=True)
    pn = Column(Numeric)
    sum_node_dose = Column(Numeric)
    nodes_on = Column(Integer)

    def __init__(self, rl_row):
        self.consumer_code = rl_row[rl['consumer_code']]
        self.hour = rl_row[rl['hour']]
        self.pn = 0
        self.sum_node_dose = 0
        self.nodes_on = 0
