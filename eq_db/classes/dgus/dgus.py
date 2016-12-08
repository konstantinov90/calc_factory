import decimal
from sqlalchemy import *
from sqlalchemy.orm import relationship, reconstructor
from utils.ORM import Base, MetaBase
from sql_scripts import dgus_script as dgs
from sql_scripts import rastr_gen_script as rgs
from .dgus_hour_data import DguHourData
from .dgus_last_hour import DguLastHour

HYDROSTATIONTYPE = 2


class Dgu(Base, metaclass=MetaBase):
    __tablename__ = 'dgus'
    id = Column(Integer, primary_key=True)
    code = Column(Integer, unique=True)
    dpg_id = Column(Integer, ForeignKey('dpg_supplies.id'))
    node_code = Column(Integer, ForeignKey('nodes.code'))
    fixed_power = Column(Numeric)

    hour_data = relationship('DguHourData', primaryjoin='Dgu.code == DguHourData.dgu_code', order_by='DguHourData.hour', lazy='subquery')
    dpg = relationship('DpgSupply', back_populates='dgus')
    gus = relationship('Gu') # , primaryjoin='Dgu.id == Gu.dgu_id') # back_populates='dgu')
    node = relationship('Node', back_populates='dgus')
    wsumgen = relationship('Wsumgen', uselist=False)
    last_hour = relationship('DguLastHour', primaryjoin='Dgu.code == DguLastHour.dgu_code', foreign_keys='DguLastHour.dgu_code', uselist=False)

    def __init__(self, dgs_row, is_new):
        self.id = dgs_row[dgs['id']]
        self.code = dgs_row[dgs['code']]
        self.dpg_id = dgs_row[dgs['dpg_id']]
        self.fixed_power = dgs_row[dgs['fixed_power']]
        self.node_code = None
        self.dgu_hour_data = {}
        self.gu_list = []
        self.is_remove = False
        self.is_new = is_new
        self._init_on_load()
        self._last_hour = None

    lst = {'id': {}, 'code': {}}
    @reconstructor
    def _init_on_load(self):
        if self.id not in self.lst['id'].keys():
            self.lst['id'][self.id] = self
        if self.code not in self.lst['code'].keys():
            self.lst['code'][self.code] = self
        self.prepared_generator_data = []
        self.sum_p = 0
        self.sum_pmax = 0

    def serialize(self, session):
        session.add(self)
        session.add_all(self.dgu_hour_data.values())
        if self._last_hour:
            session.add(self._last_hour)
        session.flush()

    def __repr__(self):
        return '<Dgu %i>' % self.code

    def set_to_remove(self):
        if not self.dgu_hour_data:
            return
        for hour in range(HOURCOUNT):
            try:
                if not sum(map(lambda x: x.gu_hour_data[hour].state if x.gu_hour_data else 0, self.gu_list)):
                    self.dgu_hour_data[hour].turn_off()
                    # print('dgu %i is turned off at hour %i' % (self.code, hour))
            except Exception:
                print
                raise Exception('dgu code %i' % self.code)

    def add_gu(self, gu):
        if gu.is_remove:
            #  уменьшаем диапазоны регулирования для РГЕ
            for hour, hd in gu.gu_hour_data.items():
                self.dgu_hour_data[hour].deplete(hd)
        else:
            self.gu_list.append(gu)

    def add_dgu_hour_data(self, rgs_row):
        hour = rgs_row[rgs['hour']]
        if isinstance(hour, decimal.Decimal):
            hour = int(hour)
        node_code = rgs_row[rgs['node_code']]
        if not self.node_code:
            self.node_code = node_code
        if node_code != self.node_code:
            raise Exception('DGU %i node_code not consistent!' % self.code)
        self.dgu_hour_data[hour] = DguHourData(rgs_row)

    def get_prepared_generator_data(self):
        if not self.prepared_generator_data:
            self.prepare_generator_data()
        return self.prepared_generator_data

    def set_parent_dpg(self, dpgs_list):
        self.dpg = dpgs_list[self.dpg_id]
        if not self.dpg:
            raise Exception('no parent DPG for DGU %i' % (dgu.code))
        self.dpg.add_dgu(self)

    def set_last_hour(self, glhs_row):
        if self._last_hour:
            raise Exception('tried to set last hour data twice!')
        self._last_hour = DguLastHour(glhs_row)

    def get_last_hour_data(self):
        if self.last_hour:
            return self.last_hour.get_data()
        return []

    def set_node(self, nodes_list):
        self.node = nodes_list[self.node_code]
        try:
            for hour, hd in self.dgu_hour_data.items():
                if hd.pmax and not self.node.get_node_hour_state(hour):
                    self.node.turn_on_hour(hour)
                    print('node %i turned on at hour %i' % (node.node_code, hour))
        except Exception:
            # raise Exception('ERROR DGU %i has no node!' % self.code)
            # traceback.print_exc()
            print('ERROR! DGU %i has no node!' % self.code)
        # if not self.node and not self.dpg.is_unpriced_zone:
        #     raise Exception('DGU %i has no corresponding node!' % self.code)

    def prepare_generator_data(self):
        if self.dpg.is_unpriced_zone or self.dpg.is_blocked or self.dpg.is_gaes:
            return  # если это неценовая зона или блок-станция или ГАЭС - возврат

        for hd in self.hour_data:
            self.sum_p += hd.p
            self.sum_pmax += hd.pmax

        if self.dpg.station.type == HYDROSTATIONTYPE and not self.sum_p:
            return  # если это РГЕ ГЭС и прогноз = 0 - возврат
        if not self.sum_pmax:
            return  # если РГЕ выключена - возврат

        for hd in self.hour_data:
            gain = 60 * (999999999 if not hd.vgain else hd.vgain)
            drop = 60 * (999999999 if not hd.vdrop else hd.vdrop)
            self.prepared_generator_data.append((
                hd.hour, self.code, 0, 0, gain, drop  # , g[rgs['wmax']], g[rgs['wmin']]
            ))
