import re
from sqlalchemy import *
from sqlalchemy.orm import relationship, reconstructor

from utils.ORM import Base, MetaBase
from sql_scripts import gus_script as gs
from sql_scripts import nblock_script as ns

from .gus_hour_data import GuHourData


class Gu(Base, metaclass=MetaBase):
    __tablename__ = 'gus'
    id = Column(Integer, primary_key=True)
    code = Column(Integer)
    dgu_id = Column(Integer, ForeignKey('dgus.id'))
    fuel_types = Column(String(50))
    fixed_power = Column(Numeric)

    hour_data = relationship('GuHourData', foreign_keys='GuHourData.gu_code', primaryjoin='GuHourData.gu_code == Gu.code', order_by='GuHourData.hour')
    dgu = relationship('Dgu', uselist=False, foreign_keys='Dgu.id', primaryjoin='Dgu.id == Gu.dgu_id' )# back_populates='gus')

    def __init__(self, row, is_in_rio):
        self.is_in_rio = is_in_rio
        self.gu_hour_data = {}
        self.dgu = None
        self.is_remove = False
        if self.is_in_rio:
            gs_row = row
            self.id = gs_row[gs['id']]
            self.code = gs_row[gs['code']]
            self.dgu_id = gs_row[gs['dgu_id']]
            ftl = gs_row[gs['fuel_type_list']]
            self.fuel_type_list = list(map(int, re.split(',', ftl))) if ftl else []
            self.fuel_types = ftl
            self.fixed_power = gs_row[gs['fixed_power']]
            self.rge_code = None
        else:
            ns_row = row
            self.code = ns_row[ns['gu_code']]
            self.id = self.code
            self.dgu_id = None
            self.fuel_type_list = []
            self.fuel_types = None
            self.fixed_power = None
            self.rge_code = ns_row[ns['dgu_code']]
        self._init_on_load()

    lst = {'id': {}, 'code': {}}
    @reconstructor
    def _init_on_load(self):
        if self.id not in self.lst['id'].keys():
            self.lst['id'][self.id] = self
        if self.code not in self.lst['code'].keys():
            self.lst['code'][self.code] = []
        if self not in self.lst['code'][self.code]:
            self.lst['code'][self.code].append(self)

    def __repr__(self):
        return '<GU id: %i / code: %i, fuel: %r, power:%r>' % (self.id, self.code, self.fuel_types, self.fixed_power)

    def set_to_remove(self):
        self.is_remove = True

    def set_parent_dgu(self, dgus_list):
        if self.is_in_rio:
            dgu = dgus_list.get_dgu_by_id(self.dgu_id)
        else:
            dgu = dgus_list.get_dgu_by_code(self.rge_code)
            self.dgu_id = dgu.id
        if self.rge_code and dgu.code != self.rge_code:
            # raise Exception('GU %i trader RGE code not consistent with rastr rge_Code!' % self.code)
            print('GU %i trader RGE code not consistent with rastr rge_Code!' % self.code)
            self.gu_hour_data = {}
        self.dgu = dgu
        try:
            dgu.add_gu(self)
        except Exception:
            print(self.rge_code, self.code, self.dgu_id)
            raise Exception('GU!')

    def add_gu_hour_data(self, ns_row):
        rge_code = ns_row[ns['dgu_code']]
        if not self.rge_code:
            self.rge_code = rge_code
        if self.rge_code != rge_code:
            raise Exception('rge_code for GU %i not consistent!' % self.code)
        self.gu_hour_data[ ns_row[ns['hour']] ] = GuHourData(ns_row)
