import time
from utils import DB, ORM
from utils.progress_bar import update_progress
from sql_scripts import rastr_load_script as rl

from .loads import Load

HOURCOUNT = 24


def make_loads(tsid={}, tdate=''):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}
    print('making loads%s' % ((' for date %s' % tdate) if tdate else ''))
    start_time = time.time()

    con = DB.OracleConnection()

    loads = LoadsList()

    @DB.process_cursor(con, rl, tsid)
    def process_loads(new_row, loads_list):
        code = new_row[rl['consumer_code']]
        load = loads_list[code]
        if not load:
            loads_list.add_load(new_row)
        loads_list[code].add_load_hour_data(new_row)

    process_loads(loads)

    for i, load in enumerate(loads):
        load.serialize(ORM.session)
        update_progress((i + 1) / len(loads))
    ORM.session.commit()
    for i, load in enumerate(loads):
        load.recalculate()
        update_progress((i + 1) / len(loads))
    ORM.session.commit()

    print('%s %s seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))

    return loads


class LoadsList(object):
    def __init__(self):
        self.loads_list = []
        self.loads_list_index = {}

    def __len__(self):
        return len(self.loads_list)

    def __iter__(self):
        for l in self.loads_list:
            yield l

    def __getitem__(self, item):
        if item in self.loads_list_index.keys():
            return self.loads_list[self.loads_list_index[item]]
        else:
            return None

    def add_load(self, rl_row):
        consumer_code = rl_row[rl['consumer_code']]
        self.loads_list_index[consumer_code] = len(self.loads_list)
        self.loads_list.append(Load(rl_row))

    def attach_nodes(self, nodes_list):
        for l in self.loads_list:
            l.attach_nodes(nodes_list)
