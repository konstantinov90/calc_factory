import time
from utils import DB
from sql_scripts import RastrLoadScript


HOURCOUNT = 24
rl = RastrLoadScript()


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


class LoadHourData(object):
    def __init__(self, rl_row):
        self.pn = rl_row[rl['pn']]
        self.node_dose = rl_row[rl['node_dose']]
        self.node_state = rl_row[rl['node_state']]
        self.k_distr = None
        self.pn_dpg_node_share = None
        self.pdem_dpg_node_share = None

    def set_k_distr(self, value):
        self.k_distr = value

    def set_pn_dpg_node_share(self, value):
        self.pn_dpg_node_share = value

    def set_pdem_dpg_node_share(self, value):
        self.pdem_dpg_node_share = value


class Load(object):
    def __init__(self, rl_row):
        self.consumer_code = rl_row[rl['consumer_code']]
        self.nodes = {}
        self.sum_pn = [0 for hour in range(HOURCOUNT)]
        self.sum_node_dose = [0 for hour in range(HOURCOUNT)]
        self.nodes_on = [0 for hour in range(HOURCOUNT)]

    def add_load_hour_data(self, rl_row):
        hour = rl_row[rl['hour']]
        node_code = rl_row[rl['node_code']]
        if not node_code in self.nodes.keys():
            self.nodes[node_code] = {'node_obj': None, 'hour_data': {}}
        if hour in self.nodes[node_code]['hour_data'].keys():
            raise Exception('tried to add load data for consmer %i at node %i twice!' % (self.code, node_code))

        load_hour_data = LoadHourData(rl_row)

        self.nodes[node_code]['hour_data'][hour] = load_hour_data

        self.sum_pn[hour] += load_hour_data.pn
        # self.sum_node_dose[hour] += load_hour_data.node_dose

    def attach_nodes(self, nodes_list):
        for node_code in self.nodes.keys():
            node = nodes_list[node_code]
            self.nodes[node_code]['node_obj'] = node
            for hour, hd in self.nodes[node_code]['hour_data'].items():
                node_state = node.get_node_hour_state(hour)
                self.sum_node_dose[hour] += hd.node_dose * node_state
                self.nodes_on[hour] += node_state
        for node_data in self.nodes.values():
            node = node_data['node_obj']
            for hour, hd in node_data['hour_data'].items():
                node_state = node.get_node_hour_state(hour)
                k_distr = (hd.node_dose * node_state / self.sum_node_dose[hour]) if self.sum_node_dose[hour] else ((node_state / self.nodes_on[hour]) if self.nodes_on[hour] else 0)
                hd.set_k_distr(k_distr)
