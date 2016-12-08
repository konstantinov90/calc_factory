from sqlalchemy import *
from utils.ORM import Base

from sql_scripts import preserves_script as prs


class DguGroupHourData(Base):
    __tablename__ = 'dgu_groups_hour_data'
    dgu_group_code = Column(Integer, ForeignKey('dgu_groups.code'), primary_key=True)
    hour = Column(Integer, primary_key=True)
    state = Column(Boolean)
    p_min = Column(Numeric)
    p_max = Column(Numeric)

    def __init__(self, prs_row):
        self.dgu_group_code = prs_row[prs['group_code']]
        self.hour = prs_row[prs['hour']]
        self.state = True if prs_row[prs['state']] else False
        self.p_min = prs_row[prs['p_min']]
        self.p_max = prs_row[prs['p_max']]
