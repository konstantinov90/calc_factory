from itertools import product
from sqlalchemy import *
from sqlalchemy.orm import relationship, reconstructor
from utils.ORM import Base, MetaBase

from sql_scripts import rge_groups_script as rgs
from sql_scripts import preserves_script as prs

from .dgu_groups_dgu_data import DguGroupDguData
from .dgu_groups_hour_data import DguGroupHourData


class DguGroup(Base, metaclass=MetaBase):
    __tablename__ = 'dgu_groups'
    code = Column(Integer, primary_key=True)

    dgu_data = relationship('DguGroupDguData', order_by='DguGroupDguData.dgu_code')
    hour_data = relationship('DguGroupHourData', order_by='DguGroupHourData.hour')

    def __init__(self, rg_row):
        self.code = rg_row[rgs['group_code']]
        self.dgus = {}
        self.hours = {}
        self._init_on_load()

    @reconstructor
    def _init_on_load(self):
        if self.code not in self.lst.keys():
            self.lst[self.code] = self
        self.constraint_data = []
        self.prepared_dgu_data = []

    def add_dgu(self, rg_row):
        dgu_code = rg_row[rgs['rge_code']]
        if dgu_code in self.dgus.keys():
            raise Exception('tried to add same dgu to group %i twice!' % self.code)
        self.dgus[dgu_code] = DguGroupDguData(rg_row)

    def add_reserve_data(self, prs_row):
        hour = prs_row[prs['hour']]
        if hour in self.hours.keys():
            raise Exception('tried to add same reserve to group %i twice!' % self.code)
        self.hours[hour] = DguGroupHourData(prs_row)
        # if prs_row[prs['state']]:  # добавляем, если состояние = 1
        #     hour = prs_row[prs['hour']]
        #     p_max = max(prs_row[prs['p_max']], 0)
        #     p_min = max(prs_row[prs['p_min']], 0)
        #     self.reserve_data[hour] = {'p_max': p_max, 'p_min': p_min}

    def get_constraint_data(self):
        if not self.constraint_data:
            self.prepare_constraint_data()
        return self.constraint_data

    def prepare_constraint_data(self):
        for hd in filter(lambda hd: hd.state, self.hour_data):
            self.constraint_data.append((
                hd.hour, self.code, hd.p_max, hd.p_min
            ))

    def get_dgu_data(self):
        if not self.prepared_dgu_data:
            self.prepare_dgu_data()
        return self.prepared_dgu_data

    def prepare_dgu_data(self):
        turned_on_hour_data = list(filter(lambda hd: hd.state, self.hour_data))
        if not turned_on_hour_data:
            return
        for hd, dgu in product(turned_on_hour_data, self.dgu_data):
            self.prepared_dgu_data.append((
                hd.hour, self.code, float(dgu.dgu_code)
            ))

    def serialize(self, session):
        session.add(self)
        session.add_all(self.hours.values())
        session.add_all(self.dgus.values())
