from sqlalchemy import *
from utils.ORM import Base
from sql_scripts import nblock_script as ns


class GuHourData(Base):
    __tablename__ = 'gus_hour_data'
    gu_code = Column(Integer, primary_key=True)
    hour = Column(Integer, primary_key=True)
    pmin = Column(Numeric)
    pmax = Column(Numeric)
    pmin_t = Column(Numeric)
    pmax_t = Column(Numeric)
    state = Column(Boolean)
    repair = Column(Integer)
    is_sysgen = Column(Boolean)
    vgain = Column(Numeric)
    vdrop = Column(Numeric)

    def __init__(self, ns_row):
        self.gu_code = ns_row[ns['gu_code']]
        self.hour = ns_row[ns['hour']]
        self.pmin = ns_row[ns['pmin']]
        self.pmax = ns_row[ns['pmax']]
        self.pmax_t = ns_row[ns['pmax_t']]
        self.pmin_t = ns_row[ns['pmin_t']]
        self.state = True if ns_row[ns['state']] else False
        self.repair = ns_row[ns['repair']]
        self.is_sysgen = True if ns_row[ns['is_sysgen']] else False
        self.vgain = ns_row[ns['vgain']]
        self.vdrop = ns_row[ns['vdrop']]
