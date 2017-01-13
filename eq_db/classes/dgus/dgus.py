"""Class Dgu."""
from operator import attrgetter
from utils.subscriptable import subscriptable
from ..meta_base import MetaBase
from .dgus_hour_data import DguHourData
from .dgus_last_hour import DguLastHour

HYDROSTATIONTYPE = 2
HOURCOUNT = 24


class Dgu(object, metaclass=MetaBase):
    """class Dgu"""
    def __init__(self, dgs_row, is_new):
        self._id, self.code, self.dpg_id, self.fixed_power = dgs_row
        self.node_code = None
        self._hour_data = {}
        self.gus = []
        self.is_remove = False
        self.is_new = is_new
        self.node = None
        self.last_hour = None
        self.wsumgen = None
        self.dpg = None
        self._init_on_load()

    @property
    def hour_data(self):
        """hour_data property"""
        return sorted(self._hour_data.values(), key=attrgetter('hour'))

    lst = {'id': {}, 'code': {}}
    def _init_on_load(self):
        """additional initialization"""
        if self._id not in self.lst['id'].keys():
            self.lst['id'][self._id] = self
        if self.code not in self.lst['code'].keys():
            self.lst['code'][self.code] = self
        self.prepared_generator_data = []
        self.sum_p = 0
        self.sum_pmax = 0

    @subscriptable
    @staticmethod
    def by_code(item):
        """get Dgu instances by code"""
        return Dgu['code', item]

    @subscriptable
    @staticmethod
    def by_id(item):
        """get Dgu instances by id"""
        return Dgu['id', item]

    def __repr__(self):
        return '<Dgu %i>' % self.code

    def set_to_remove(self):
        """set instance to remove"""
        if not self.hour_data:
            return
        for hour in range(HOURCOUNT):
            try:
                if not sum(1 if gu.hour_data[hour].state else 0 if gu.hour_data else 0
                           for gu in self.gus):
                    self.hour_data[hour].turn_off()
                    # print('dgu %i is turned off at hour %i' % (self.code, hour))
            except Exception:
                raise Exception('dgu code %i' % self.code)

    def add_gu(self, gen_unit):
        """add Gu instance"""
        if gen_unit.is_remove:
            #  уменьшаем диапазоны регулирования для РГЕ
            for _hd in gen_unit.hour_data:
                self.hour_data[_hd.hour].deplete(_hd)
        else:
            self.gus.append(gen_unit)

    def add_dgu_hour_data(self, rgs_row):
        """add DguHourData instance"""
        hour = rgs_row.hour
        # if isinstance(hour, decimal.Decimal):
        #     hour = int(hour)
        node_code = rgs_row.node_code
        if not self.node_code:
            self.node_code = node_code
        if node_code != self.node_code:
            raise Exception('DGU %i node_code not consistent!' % self.code)
        self._hour_data[hour] = DguHourData(rgs_row)

    def get_prepared_generator_data(self):
        """get eq_db_generators view data"""
        if not self.prepared_generator_data:
            self.prepare_generator_data()
        return self.prepared_generator_data

    def set_parent_dpg(self, dpgs_list):
        """set Dpg instance"""
        self.dpg = dpgs_list.by_id[self.dpg_id]
        if not self.dpg:
            raise Exception('no parent DPG for DGU %i' % (self.code))
        self.dpg.add_dgu(self)

    def set_last_hour(self, glhs_row):
        """set DguLastHour instance"""
        if self.last_hour:
            raise Exception('tried to set last hour data twice!')
        self.last_hour = DguLastHour(glhs_row)

    def set_node(self, nodes_list):
        """set Node instance"""
        self.node = nodes_list[self.node_code]
        if self.node:
            self.node.add_dgu(self)

    def set_wsumgen(self, wsumgen_list):
        """set Wsumgen instance"""
        dummy = [ws for ws in wsumgen_list if ws.dgu_code == self.code]
        if len(dummy) > 1:
            raise Exception('too many wsumgen for dgu %i' % self.code)
        if dummy:
            self.wsumgen = dummy[0]

    def get_last_hour_data(self):
        """get DguLastHour instance"""
        if self.last_hour:
            return self.last_hour.get_data()
        return []

    def prepare_generator_data(self):
        """prepare eq_db_generators view data"""
        if self.dpg.is_unpriced_zone: # or self.dpg.is_gaes or self.dpg.is_blocked:
            return  # если это неценовая зона или блок-станция или ГАЭС - возврат

        for _hd in self.hour_data:
            self.sum_p += _hd.p
            self.sum_pmax += _hd.pmax

        if self.dpg.station.type == HYDROSTATIONTYPE and not self.sum_p:
            return  # если это РГЕ ГЭС и прогноз = 0 - возврат
        if not self.sum_pmax:
            return  # если РГЕ выключена - возврат

        for _hd in self.hour_data:
            gain = 60 * (999999999 if not _hd.vgain else _hd.vgain)
            drop = 60 * (999999999 if not _hd.vdrop else _hd.vdrop)
            self.prepared_generator_data.append((
                _hd.hour, self.code, 0, 0, gain, drop  # , g[rgs['wmax']], g[rgs['wmin']]
            ))
