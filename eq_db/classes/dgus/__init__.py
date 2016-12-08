import traceback
import time
from utils import DB, ORM
from utils.progress_bar import update_progress
from operator import itemgetter
from sql_scripts import dgus_script as dgs
from sql_scripts import rastr_gen_script as rgs
from sql_scripts import generators_last_hour_script as glhs
from .dgus import Dgu

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

    @DB.process_cursor(con, glhs, tsid)
    def process_last_hour(new_row, dgus_list):
        dgu_code = new_row[glhs['rge_code']]
        dgu = dgus_list.get_dgu_by_code(dgu_code)
        if dgu:
            dgu.set_last_hour(new_row)

    process_dgus(dgus)
    process_dgu_data(dgus)
    process_last_hour(dgus)

    for i, dgu in enumerate(dgus):
        dgu.serialize(ORM.session)
        update_progress((i + 1) / len(dgus))
    ORM.session.commit()
    
    print('%s %s seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))

    return dgus


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
