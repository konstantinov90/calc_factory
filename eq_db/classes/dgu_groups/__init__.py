import time
from operator import itemgetter
from itertools import product
from utils import DB, ORM
from sql_scripts import rge_groups_script as rgs
from sql_scripts import preserves_script as prs

from .dgu_groups import DguGroup

HOURCOUNT = 24


def make_dgu_groups(tsid, tdate=''):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}
    print('making rge_groups%s' % ((' for date %s' % tdate) if tdate else ''))
    start_time = time.time()

    con = DB.OracleConnection()

    dgu_groups = DguGroupsList()

    @DB.process_cursor(con, rgs, tsid)
    def process_dgu_groups(new_row, dgu_groups_list):
        group_code = new_row[rgs['group_code']]
        if not dgu_groups_list[group_code]:
            dgu_groups_list.add_dgu_group(new_row)
        dgu_groups_list[group_code].add_dgu(new_row)

    @DB.process_cursor(con, prs, tsid)
    def process_dgu_groups_reserves(new_row, dgu_groups_list):
        group_code = new_row[prs['group_code']]
        dgu_group = dgu_groups_list[group_code]
        if dgu_group:
            dgu_group.add_reserve_data(new_row)

    # print('loading dgu_groups information')
    process_dgu_groups(dgu_groups)
    process_dgu_groups_reserves(dgu_groups)
    for dgu_group in dgu_groups:
        dgu_group.serialize(ORM.session)
    ORM.session.commit()

    print('%s %s seconds %s' % (15 * '-',  round(time.time() - start_time, 3), 15 * '-'))
    return dgu_groups


class DguGroupsList(object):
    def __init__(self):
        self.dgu_groups_list = []
        self.dgu_groups_list_index = {}
        self.constraints_data = [[(0, 1, -1.)] for hour in range(HOURCOUNT)]
        self.constraint_dgus_data = [[(0, 0.)] for hour in range(HOURCOUNT)]

    def __len__(self):
        return len(self.dgu_groups_list)

    def add_dgu_group(self, rgs_row):
        group_code = rgs_row[rgs['group_code']]
        self.dgu_groups_list_index[group_code] = len(self.dgu_groups_list)
        self.dgu_groups_list.append(DguGroup(rgs_row))

    def __getitem__(self, item):
        if item in self.dgu_groups_list_index.keys():
            return self.dgu_groups_list[self.dgu_groups_list_index[item]]
        else:
            return None

    def __iter__(self):
        for n in self.dgu_groups_list:
            yield n

    def get_prepared_constraints_data(self):
        if len(self.constraints_data[0]) == 1:
            self.prepare_constraints_data()
        return self.constraints_data

    def prepare_constraints_data(self):
        c_s = {'hour': 0, 'group_code': 0}
        h_int = c_s['hour']
        for rge_group in self.dgu_groups_list:
            [self.constraints_data[d[h_int]].append(d[:h_int] + d[(h_int + 1):])
                for d in dgu_group.get_constraint_data()]
        [l.sort(key=itemgetter(c_s['group_code'])) for l in self.constraints_data]

    def get_prepared_constraint_dgus_data(self):
        if len(self.constraint_dgus_data[0]) == 1:
            self.prepare_constraint_dgus_data()
        return self.constraint_dgus_data

    def prepare_constraint_dgus_data(self):
        c_s = {'hour': 0, 'group_code': 0, 'rge_code': 1}
        h_int = c_s['hour']
        for rge_group in self.dgu_groups_list:
            [self.constraint_dgus_data[d[h_int]].append(d[:h_int] + d[(h_int + 1):])
                for d in dgu_group.get_rge_data()]
        [l.sort(key=itemgetter(c_s['group_code'], c_s['rge_code'])) for l in self.constraint_dgus_data]
