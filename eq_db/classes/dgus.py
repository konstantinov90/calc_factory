import traceback
import time
from utils import DB
from operator import itemgetter
import decimal
from sql_scripts import DGUsScript
from sql_scripts import RastrGenScript

dgs = DGUsScript()
rgs = RastrGenScript()

HOURCOUNT = 24
HYDROSTATIONTYPE = 2


def make_dgus(tsid={}, tdate=''):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}
    print('making dgus%s' % ((' for date %s' % tdate) if tdate else ''))
    start_time = time.time()

    con = DB.OracleConnection()

    dgus = DgusList()

    @DB.process_cursor(con, dgs, tsid)
    def process_dgus(new_row, dgus_list):
        dgus_list.add_dgu(new_row)

    @DB.process_cursor(con, rgs, tsid)
    def process_dgu_data(new_row, dgus_list):
        dgu_code = new_row[rgs['rge_code']]
        dgu = dgus_list.get_dgu_by_code(dgu_code)
        if dgu:
            dgu.add_dgu_hour_data(new_row)

    process_dgus(dgus)
    process_dgu_data(dgus)

    print('%s %s seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))

    return dgus


class DguHourData(object):
    def __init__(self, rgs_row):
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


class DgusList(object):
    def __init__(self):
        self.dgus_list = []
        self.dgus_list_index_by_id = {}
        self.dgus_list_index_by_code = {}
        self.prepared_generator_data = [[] for hour in range(HOURCOUNT)]

    def __len__(self):
        return len(self.dgus_list)

    def __iter__(self):
        for n in self.dgus_list:
            yield n

    def add_dgu(self, dgs_row, is_new_dgu=False):
        dgu_code = dgs_row[dgs['code']]
        dgu_id = dgs_row[dgs['id']]
        N = len(self.dgus_list)
        self.dgus_list_index_by_id[dgu_id] = N
        self.dgus_list_index_by_code[dgu_code] = N
        self.dgus_list.append(Dgu(dgs_row, is_new_dgu))

    def get_dgu_by_id(self, id):
        return self._get_item_by_index(self.dgus_list, self.dgus_list_index_by_id, id)

    def get_dgu_by_code(self, code):
        return self._get_item_by_index(self.dgus_list, self.dgus_list_index_by_code, code)

    def set_parent_dpgs(self, dpgs_list):
        for dgu in self.dgus_list:
            dgu.set_parent_dpg(dpgs_list)

    def set_nodes(self, nodes_list):
        for dgu in self.dgus_list:
            dgu.set_node(nodes_list)

    def set_to_remove(self):
        for dgu in self.dgus_list:
            dgu.set_to_remove()

    @staticmethod
    def _get_item_by_index(lst, index, item):
        if item in index.keys():
            return lst[index[item]]
        else:
            return None

    def get_prepared_generator_data(self):
        if not self.prepared_generator_data[0]:
            self.prepare_generator_data()
        return self.prepared_generator_data

    def prepare_generator_data(self):
        g_i = {'hour': 0, 'rge_code': 0}
        h_g_i = g_i['hour']
        for dgu in self.dgus_list:
            [self.prepared_generator_data[d[h_g_i]].append(d[:h_g_i] + d[(h_g_i + 1):])
                for d in dgu.get_prepared_generator_data()]
        [l.sort(key=itemgetter(g_i['rge_code'])) for l in self.prepared_generator_data]


class Dgu(object):
    def __init__(self, dgs_row, is_new):
        self.id = dgs_row[dgs['id']]
        self.code = dgs_row[dgs['code']]
        self.dpg_id = dgs_row[dgs['dpg_id']]
        self.node_code = None
        self.dpg = None
        self.node = None
        self.fixed_power = dgs_row[dgs['fixed_power']]
        self.dgu_hour_data = {}
        self.sum_p = 0
        self.sum_pmax = 0
        self.gu_list = []
        self.prepared_generator_data = []
        self.is_remove = False
        self.is_new = is_new

    def __str__(self):
        return '%i' % self.code

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

        for hour_data in self.dgu_hour_data.values():
            self.sum_p += hour_data.p
            self.sum_pmax += hour_data.pmax

        if self.dpg.station.type == HYDROSTATIONTYPE and not self.sum_p:
            return  # если это РГЕ ГЭС и прогноз = 0 - возврат
        if not self.sum_pmax:
            return  # если РГЕ выключена - возврат

        for hour, hd in self.dgu_hour_data.items():
            gain = 60 * (999999999 if not hd.vgain else hd.vgain)
            drop = 60 * (999999999 if not hd.vdrop else hd.vdrop)
            self.prepared_generator_data.append((
                hour, self.code, 0, 0, gain, drop  # , g[rgs['wmax']], g[rgs['wmin']]
            ))
