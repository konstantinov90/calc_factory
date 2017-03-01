"""Class Dgu."""
from operator import attrgetter
from functools import partial
from utils.subscriptable import subscriptable
from ..meta_base import MetaBase
from .dgus_hour_data import DguHourData
from .dgus_last_hour import DguLastHour

HYDROSTATIONTYPE = 2
HOURCOUNT = 24


class Dgu(object, metaclass=MetaBase):
    """class Dgu"""
    def __init__(self, dgs_row, is_new=False):
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
        self.kg_fixed = None
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

    # def deplete(self):
    #     for gu in self.gus:
    #         if gu.is_remove:
    #             for _hd in self.hour_data:
    #                 _hd.deplete(gu.hour_data[_hd.hour])

    def modify_state(self):
        """set instance to remove"""
        if self.dpg.is_unpriced_zone or self.dpg.station.type == HYDROSTATIONTYPE:
            return
        turned_off = False
        for _hd in self.hour_data:
            delayed_augment = []
            dgu_state = 0
            for gu_hd in {_gu.hour_data[_hd.hour] for _gu in self.gus if _gu.hour_data}:
                if gu_hd.changed:
                    if gu_hd.state:
                        delayed_augment.append(partial(_hd.augment, gu_hd))
                    else:
                        _hd.deplete(gu_hd)
                else:
                    dgu_state += gu_hd.state
            if not dgu_state:
                _hd.turn_off()

            for func in delayed_augment:
                func()

            if not delayed_augment:
                if not turned_off:
                    # print('dgu %i is turned off at hour(s)' % self.code, end='')
                    turned_off = True
                # print(' %i' % _hd.hour, end='')
            else:
                if not self.node.hour_data[_hd.hour].state:
                    raise Exception('cannot turn on dgu %i in turned off node %i'
                                    % (self.code, self.node.code))
        # if turned_off:
        #     print('')

    def add_gu(self, gen_unit):
        """add Gu instance"""
        # if gen_unit.is_remove:
        #     #  уменьшаем диапазоны регулирования для РГЕ
        #     for _hd in gen_unit.hour_data:
        #         self.hour_data[_hd.hour].deplete(_hd)
        # else:
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
        self._hour_data[hour] = DguHourData(rgs_row, self)

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
        if dummy:
            try:
                (dummy,) = dummy
            except ValueError:
                print('too many wsumgen for dgu %i' % self.code)
                raise
            self.wsumgen = dummy
            self.wsumgen.set_dgu(self)

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
            node_code = self.node.code if self.node.hour_data[_hd.hour].state else 0
            self.prepared_generator_data.append((
                _hd.hour, self.code, 0, 0, gain, drop, _hd.pmin, _hd.pmax,
                self.dpg.station.type, node_code
            ))

    def fill_db(self, con):
        """fill kg_dpg_rge"""
        script = """INSERT into kg_dpg_rge (hour, kg, p, pmax, pmin, pminagg, dpminso,
                                kg_min, kg_reg, dpmin_heat, dpmin_tech, dpg_id, node,
                                rge_id, rge_code, sta, kg_fixed_power)
                    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14,
                            :15, :16, :17)"""
        if not self.hour_data:
            return
        data = []
        common_data = (self.dpg_id, self.node.code, self._id, self.code)
        for _hd in self.hour_data:
            node_state = self.node.get_node_hour_state(_hd.hour)
            data.append(_hd.get_insert_data() + common_data +
                        ((0, self.kg_fixed) if node_state else (1, None)))

        # attrs = attrgetter(*'''hour dgu_code pmin pmax pmin_agg pmax_agg pmin_tech
        #                       pmax_tech pmin_heat pmax_heat pmin_so pmax_so p wmax
        #                       wmin vgain vdrop'''.split())
        # gen_script = """INSERT into rastr_generator (hour, o$num, o$pmin, o$pmax, o$pminagg,
        #                         o$pmaxagg, o$dpmintech, o$dpmaxtech, o$dpminheat,
        #                         o$dpmaxheat, o$dpminso, o$dpmaxso, o$p, o$wmax, o$wmin,
        #                         o$vgain, o$vdrop, o$node)
        #             VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13,
        #                     :14, :15, :16, :17, :18)"""

        with con.cursor() as curs:
            curs.executemany(script, data)
            # curs.execute('DELETE from rastr_generator where o$num = %i' % self.code)
            # curs.executemany(gen_script, [attrs(_hd) + (self.node.code,)
            #                               for _hd in self.hour_data])
