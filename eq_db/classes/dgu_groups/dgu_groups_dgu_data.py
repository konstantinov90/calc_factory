from sqlalchemy import *
from utils.ORM import Base

from sql_scripts import rge_groups_script as rgs


class DguGroupDguData(Base):
    __tablename__ = 'dgu_groups_dgu_data'
    dgu_group_code = Column(Integer, ForeignKey('dgu_groups.code'), primary_key=True)
    dgu_code = Column(Integer, primary_key=True)

    def __init__(self, rgs_row):
        self.dgu_group_code = rgs_row[rgs['group_code']]
        self.dgu_code = rgs_row[rgs['rge_code']]
