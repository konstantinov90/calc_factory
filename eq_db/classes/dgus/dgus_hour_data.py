from sqlalchemy import *
from utils.ORM import Base

from sql_scripts import rastr_gen_script as rgs


class DguHourData(Base):
    __tablename__ = 'dgus_hour_data'
    dgu_code = Column(Integer, ForeignKey('dgus.code'), primary_key=True)
    hour = Column(Integer, primary_key=True)
    pmin = Column(Numeric)
    pmax = Column(Numeric)
    pmin_agg = Column(Numeric)
    pmax_agg = Column(Numeric)
    pmin_tech = Column(Numeric)
    pmax_tech = Column(Numeric)
    pmin_heat = Column(Numeric)
    pmax_heat = Column(Numeric)
    pmin_so = Column(Numeric)
    pmax_so = Column(Numeric)
    p = Column(Numeric)
    wmin = Column(Numeric)
    wmax = Column(Numeric)
    vgain = Column(Numeric)
    vdrop = Column(Numeric)
    pmin_technological = Column(Numeric)

    def __init__(self, rgs_row):
        self.dgu_code = rgs_row[rgs['rge_code']]
        self.hour = rgs_row[rgs['hour']]
        self.pmin = rgs_row[rgs['pmin']]
        self.pmax = rgs_row[rgs['pmax']]
        self.pmin_agg = rgs_row[rgs['pmin_agg']]
        self.pmax_agg = rgs_row[rgs['pmax_agg']]
        self.pmin_tech = rgs_row[rgs['pmin_tech']]
        self.pmax_tech = rgs_row[rgs['pmax_tech']]
        self.pmin_heat = rgs_row[rgs['pmin_heat']]
        self.pmax_heat = rgs_row[rgs['pmax_heat']]
        self.pmin_so = rgs_row[rgs['pmin_so']]
        self.pmax_so = rgs_row[rgs['pmax_so']]
        self.p = rgs_row[rgs['p']]
        self.wmax = rgs_row[rgs['wmax']]
        self.wmin = rgs_row[rgs['wmin']]
        self.vgain = rgs_row[rgs['vgain']]
        self.vdrop = rgs_row[rgs['vdrop']]
        self.pmin_technological = max(self.pmin, self.pmin_heat, self.pmin_tech)

    def deplete(self, hd):
        '''hd == gu_hour_data'''
        if hd.state:
            self.pmin -= min(hd.pmin, self.pmin)
            self.pmin_agg -= min(hd.pmin, self.pmin_agg)
            self.pmin_tech -= min(hd.pmin, self.pmin_tech)
            self.pmin_heat -= min(hd.pmin, self.pmin_heat)
            self.pmin_so -= min(hd.pmin, self.pmin_so)

            self.p -= min(hd.pmax, self.p)

            self.pmax -= min(hd.pmax, self.pmax)
            self.pmax_agg -= min(hd.pmax, self.pmax_agg)
            self.pmax_tech -= min(hd.pmax, self.pmax_tech)
            self.pmax_heat -= min(hd.pmax, self.pmax_heat)
            self.pmax_so -= min(hd.pmax, self.pmax_so)

            self.vdrop -= min(hd.vdrop, self.vdrop)
            self.vgain -= min(hd.vgain, self.vgain)

            self.pmin_technological = max(self.pmin, self.pmin_heat, self.pmin_tech)

    def turn_off(self):
        self.pmin = 0
        self.pmax = 0
        self.pmin_agg = 0
        self.pmax_agg = 0
        self.pmin_tech = 0
        self.pmax_tech = 0
        self.pmin_heat = 0
        self.pmax_heat = 0
        self.pmin_so = 0
        self.pmax_so = 0
        self.p = 0
        self.wmax = 0
        self.wmin = 0
        self.vgain = 0
        self.vdrop = 0
        self.pmin_technological = 0
