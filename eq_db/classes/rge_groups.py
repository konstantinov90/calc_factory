import time
from operator import itemgetter
from itertools import product
from utils import DB
from sql_scripts import RgeGroupsScript
from sql_scripts import PReservesScript

HOURCOUNT = 24

rgs = RgeGroupsScript()
prs = PReservesScript()


def make_rge_groups(tsid, tdate=''):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}
    print('making rge_groups%s' % ((' for date %s' % tdate) if tdate else ''))
    start_time = time.time()

    con = DB.OracleConnection()

    rge_groups = RgeGroupsList()

    @DB.process_cursor(con, rgs, tsid)
    def process_rge_groups(new_row, rge_groups_list):
        group_code = new_row[rgs['group_code']]
        if rge_groups_list[group_code]:
            rge_groups_list[group_code].add_rge(new_row)
        else:
            rge_groups_list.add_rge_group(new_row)
            rge_groups_list[group_code].add_rge(new_row)

    @DB.process_cursor(con, prs, tsid)
    def process_rge_groups_reserves(new_row, rge_groups_list):
        group_code = new_row[prs['group_code']]
        rge_group = rge_groups_list[group_code]
        if rge_group:
            rge_group.add_reserve_data(new_row)

    # print('loading rge_groups information')
    process_rge_groups(rge_groups)
    process_rge_groups_reserves(rge_groups)

    print('%s %s seconds %s' % (15 * '-',  round(time.time() - start_time, 3), 15 * '-'))
    return rge_groups


class RgeGroupsList(object):
    def __init__(self):
        self.rge_groups_list = []
        self.rge_groups_list_index = {}
        self.constraints_data = [[(0, 1, -1.)] for hour in range(HOURCOUNT)]
        self.constraint_rges_data = [[(0, 0.)] for hour in range(HOURCOUNT)]

    def __len__(self):
        return len(self.rge_groups_list)

    def add_rge_group(self, rgs_row):
        group_code = rgs_row[rgs['group_code']]
        self.rge_groups_list_index[group_code] = len(self.rge_groups_list)
        self.rge_groups_list.append(RgeGroup(rgs_row))

    def __getitem__(self, item):
        if item in self.rge_groups_list_index.keys():
            return self.rge_groups_list[self.rge_groups_list_index[item]]
        else:
            return None

    def __iter__(self):
        for n in self.rge_groups_list:
            yield n

    def get_prepared_constraints_data(self):
        if len(self.constraints_data[0]) == 1:
            self.prepare_constraints_data()
        return self.constraints_data

    def prepare_constraints_data(self):
        c_s = {'hour': 0, 'group_code': 0}
        h_int = c_s['hour']
        for rge_group in self.rge_groups_list:
            [self.constraints_data[d[h_int]].append(d[:h_int] + d[(h_int + 1):])
                for d in rge_group.get_constraint_data()]
        [l.sort(key=itemgetter(c_s['group_code'])) for l in self.constraints_data]

    def get_prepared_constraint_rges_data(self):
        if len(self.constraint_rges_data[0]) == 1:
            self.prepare_constraint_rges_data()
        return self.constraint_rges_data

    def prepare_constraint_rges_data(self):
        c_s = {'hour': 0, 'group_code': 0, 'rge_code': 1}
        h_int = c_s['hour']
        for rge_group in self.rge_groups_list:
            [self.constraint_rges_data[d[h_int]].append(d[:h_int] + d[(h_int + 1):])
                for d in rge_group.get_rge_data()]
        [l.sort(key=itemgetter(c_s['group_code'], c_s['rge_code'])) for l in self.constraint_rges_data]


class RgeGroup(object):
    def __init__(self, rg_row):
        self.group_code = rg_row[rgs['group_code']]
        self.rges = []
        self.reserve_data = {}
        self.constraint_data = []
        self.rge_data = []

    def add_rge(self, rg_row):
        rge_code = rg_row[rgs['rge_code']]
        if rge_code in self.rges:
            raise Exception('tried to add same rge to group %i twice!' % self.group_code)
        self.rges.append(rge_code)

    def add_reserve_data(self, prs_row):
        if prs_row[prs['state']]:  # добавляем, если состояние = 1
            hour = prs_row[prs['hour']]
            p_max = max(prs_row[prs['p_max']], 0)
            p_min = max(prs_row[prs['p_min']], 0)
            self.reserve_data[hour] = {'p_max': p_max, 'p_min': p_min}

    def get_constraint_data(self):
        if not self.constraint_data:
            self.prepare_constraint_data()
        return self.constraint_data

    def prepare_constraint_data(self):
        for hour in self.reserve_data.keys():
            self.constraint_data.append((
                hour, self.group_code, self.reserve_data[hour]['p_max'], self.reserve_data[hour]['p_min']
            ))

    def get_rge_data(self):
        if not self.rge_data:
            self.prepare_rge_data()
        return self.rge_data

    def prepare_rge_data(self):
        if not self.reserve_data:
            return
        for hour, rge_code in product(self.reserve_data.keys(), self.rges):
            self.rge_data.append((
                hour, self.group_code, float(rge_code)
            ))
