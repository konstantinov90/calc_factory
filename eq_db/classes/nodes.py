from operator import itemgetter
import time
from sql_scripts import NodesScript
from sql_scripts import ImpexAreaScript
from utils import DB
from utils.progress_bar import update_progress
import settings


ns = NodesScript()
ias = ImpexAreaScript()
HOURCOUNT = 24


def make_nodes(tsid={}, tdate=''):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}
    print('making nodes%s' % ((' for date %s' % tdate) if tdate else ''))
    start_time = time.time()

    con = DB.OracleConnection()

    nodes = NodesList()

    @DB.process_cursor(con, ns, tsid)
    def process_nodes(new_row, node_list):
        node_code = new_row[ns['node_code']]
        if not node_list[node_code]:
            node_list.add_node(new_row)
        node_list[node_code].add_node_hour_data(new_row)

    # print('loading nodes information')
    process_nodes(nodes)

    for i, node in enumerate(nodes):
        if not i:  # i == 0
            node.set_impex_area(con.exec_script(ias.get_query(), tsid))
        node.prepare_data()
        update_progress((i + 1) / len(nodes))

    # [print(n) for n in nodes[nodes_index[1001068]].get_eq_db_nodes_data()]

    print('%s %i seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))

    return nodes


class NodeHourData(object):
    def __init__(self, ns_row):
        self.state = ns_row[ns['state']]
        self.type = ns_row[ns['type']]
        self.voltage = ns_row[ns['voltage']]
        self.fixed_voltage = ns_row[ns['fixed_voltage']]
        self.phase = 3.1415 * (((ns_row[ns['phase']] + 180) % 360) - 180) / 180
        self.pn = ns_row[ns['pn']]
        self.pg = ns_row[ns['pg']]
        self.qn = ns_row[ns['qn']]
        self.qg = ns_row[ns['qg']]
        self.min_q = ns_row[ns['min_q']]
        self.max_q = ns_row[ns['max_q']]
        self.g_shunt = ns_row[ns['g_shunt']]
        self.b_shunt = ns_row[ns['b_shunt']]
        self.p_shunt = ns_row[ns['p_shunt']]
        self.q_shunt = ns_row[ns['q_shunt']]
        self.b_shr = ns_row[ns['b_shr']]
        self.pdem = 0
        self.retail = 0
        self.k_distr = None
        self.balance_node_code = None

    def set_balance_node(self, node_code):
        self.balance_node_code = node_code

    def is_node_on(self):
        return not self.state

    def turn_on(self):
        self.state = 0

    def is_balance_node(self):
        return not self.type

    def get_eq_db_node_hour(self):
        return self.voltage, self.phase

    def add_to_pdem(self, value):
        self.pdem += value

    def add_to_retail(self, value):
        self.retail += value

    def set_k_distr(self, value):
        self.k_distr = value


class NodesList(object):
    def __init__(self):
        self.nodes_list = []
        self.nodes_list_index = {}
        self.nodes_data = [[] for hour in range(HOURCOUNT)]
        self.nodes_pq_data = [[] for hour in range(HOURCOUNT)]
        self.nodes_pv_data = [[] for hour in range(HOURCOUNT)]
        self.nodes_sw_data = [[] for hour in range(HOURCOUNT)]
        self.shunts_data = [[] for hour in range(HOURCOUNT)]
        self.dpgs_list = None
        self.balance_nodes = [[] for hour in range(HOURCOUNT)]

    def __len__(self):
        return len(self.nodes_list)

    def add_node(self, ns_row):
        node_code = ns_row[ns['node_code']]
        self.nodes_list_index[node_code] = len(self.nodes_list)
        self.nodes_list.append(Node(ns_row))

    def append(self, node):
        self.nodes_list_index[node.node_code] = len(self.nodes_list)
        self.nodes_list.append(node)

    def __iter__(self):
        for n in self.nodes_list:
            yield n

    def __getitem__(self, item):
        if item in self.nodes_list_index.keys():
            return self.nodes_list[self.nodes_list_index[item]]
        else:
            return None

    def get_nodes_by_area(self, item):
        ret = []
        for node in self.nodes_list:
            if node.area == item:
                ret.append(node)
        return ret

    def attach_dpgs(self, dpgs_list):
        self.dpgs_list = dpgs_list

    def get_prepared_shunts_data(self):
        if not self.shunts_data[0]:
            self.prepare_shunts_data()
        return self.shunts_data

    def prepare_shunts_data(self):
        n_s = {'hour': 0, 'node_code': 0}
        h_ind = n_s['hour']
        for node in self.nodes_list:
            [self.shunts_data[d[h_ind]].append(d[:h_ind] + d[(h_ind + 1):])
                for d in node.get_shunt_data()]
        [l.sort(key=itemgetter(n_s['node_code'])) for l in self.shunts_data]

    def get_prepared_nodes_data(self):
        if not self.nodes_data[0]:
            self.prepare_eq_db_nodes_data()
        return self.nodes_data

    def prepare_eq_db_nodes_data(self):
        n_s = {'hour': 0, 'node_code': 0}
        h_ind = n_s['hour']
        for node in self.nodes_list:
            [self.nodes_data[d[h_ind]].append(d[:h_ind] + d[(h_ind + 1):])
                for d in node.get_eq_db_nodes_data()]
        [l.sort(key=itemgetter(n_s['node_code'])) for l in self.nodes_data]

    def get_prepared_nodes_pq_data(self):
        if not self.nodes_pq_data[0]:
            self.prepare_nodes_data()
        return self.nodes_pq_data

    def get_prepared_nodes_pv_data(self):
        if not self.nodes_pv_data[0]:
            self.prepare_nodes_data()
        return self.nodes_pv_data

    def get_prepared_nodes_sw_data(self):
        if not self.nodes_sw_data[0]:
            self.prepare_nodes_data()
        return self.nodes_sw_data

    def prepare_nodes_data(self):
        nodes_pq_index = {}
        nodes_pv_index = {}
        ns_pq = {'node_code':0, 'p_cons_minus_gen': 2, 'cons': 7, 'gen': 8}
        ns_pv = {'node_code':0, 'p_gen': 2, 'cons': 11, 'gen': 12}
        ns_sw = {'node_code': 0}
        fis = {'hour': 0, 'node_code': 1, 'value': 2}

        # обработка fixedimpex
        for i, node in enumerate(self.nodes_list):
            node_pq_index = {}
            node_pv_index = {}
            fixedimpex = node.get_impex_data()

            for hour, node_hour in node.hour_data.items():
                if not node_hour.is_node_on():
                    continue
                fixedimpex_hour = [f for f in fixedimpex if f[fis['hour']] == hour]
                if len(fixedimpex_hour) > 1:
                    raise Exception('wrong fixedimpex count for node %i' % node.node_code)
                if fixedimpex_hour:
                    impex_value = fixedimpex_hour[0][fis['value']]
                else:
                    impex_value = 0

                if node_hour.type == 1:
                    node_pq_index[hour] = len(self.nodes_pq_data[hour])
                    self.nodes_pq_data[hour].append([
                        node.node_code,
                        node.voltage_class,
                        impex_value,
                        node_hour.qn,
                        node_hour.qg,
                        1.4 * node.voltage_class,
                        0.75 * node.voltage_class,
                        max(impex_value, 0),
                        -1 * min(impex_value, 0)
                    ])
                elif node_hour.type > 1:
                    node_pv_index[hour] = len(self.nodes_pv_data[hour])
                    self.nodes_pv_data[hour].append([
                        node.node_code,
                        node.voltage_class,
                        impex_value,
                        node_hour.qn,
                        node_hour.qg,
                        node_hour.type,
                        node_hour.fixed_voltage,
                        max(node_hour.max_q, -10000),
                        min(node_hour.min_q, 10000),
                        1.4 * node.voltage_class,
                        0.75 * node.voltage_class,
                        -1 * min(impex_value, 0),
                        max(impex_value, 0)
                    ])
                elif node_hour.is_balance_node():
                    relative_voltage = (node_hour.fixed_voltage if node_hour.fixed_voltage else node.nominal_voltage)\
                                                                                     / node.voltage_class
                    self.nodes_sw_data[hour].append((
                        node.node_code,
                        node.voltage_class,
                        relative_voltage,
                        0,
                        -node_hour.pn,
                        max(node_hour.max_q, -10000),
                        min(node_hour.min_q, 10000)
                    ))

            if node_pq_index:
                nodes_pq_index[node.node_code] = node_pq_index
            if node_pv_index:
                nodes_pv_index[node.node_code] = node_pv_index

        # обработка fixedgen
        fgs = {'hour': 0, 'node_code': 2, 'value': 4}
        for i, dpg in enumerate(self.dpgs_list):
            for fixedgen in dpg.get_fixedgen_data():
                node_code = fixedgen[fgs['node_code']]
                hour = fixedgen[fgs['hour']]
                if node_code in nodes_pq_index.keys():
                    ni = nodes_pq_index[node_code]
                    if hour not in ni.keys():
                        continue
                    self.nodes_pq_data[hour][ni[hour]][ns_pq['p_cons_minus_gen']] += fixedgen[fgs['value']]
                    self.nodes_pq_data[hour][ni[hour]][ns_pq['gen']] += -fixedgen[fgs['value']]

                elif node_code in nodes_pv_index.keys():
                    ni = nodes_pv_index[node_code]
                    if hour not in ni.keys():
                        continue
                    self.nodes_pv_data[hour][ni[hour]][ns_pv['p_gen']] += fixedgen[fgs['value']]
                    self.nodes_pv_data[hour][ni[hour]][ns_pv['gen']] += fixedgen[fgs['value']]

        # обработка fixedcon
        fcs = {'hour': 0, 'node_code': 1, 'value': 3}
        for i, dpg in enumerate(self.dpgs_list):
            for fixedcon in dpg.get_fixedcon_data():
                node_code = fixedcon[fcs['node_code']]
                hour = fixedcon[fcs['hour']]
                if node_code in nodes_pq_index.keys():
                    ni = nodes_pq_index[node_code]
                    if hour not in ni.keys():
                        continue
                    self.nodes_pq_data[hour][ni[hour]][ns_pq['p_cons_minus_gen']] += fixedcon[fcs['value']]
                    self.nodes_pq_data[hour][ni[hour]][ns_pq['cons']] += fixedcon[fcs['value']]

                elif node_code in nodes_pv_index.keys():
                    ni = nodes_pv_index[node_code]
                    if hour not in ni.keys():
                        continue
                    self.nodes_pv_data[hour][ni[hour]][ns_pv['p_gen']] += fixedcon[fcs['value']]
                    self.nodes_pv_data[hour][ni[hour]][ns_pv['cons']] += -fixedcon[fcs['value']]
        [l.sort(key=itemgetter(ns_pq['node_code'])) for l in self.nodes_pq_data]
        [l.sort(key=itemgetter(ns_pv['node_code'])) for l in self.nodes_pv_data]
        [l.sort(key=itemgetter(ns_sw['node_code'])) for l in self.nodes_sw_data]

    def set_balance_nodes(self, lines_list):
        self.locate_balance_nodes()
        for hour, balance_nodes in enumerate(self.balance_nodes):
            # if hour:
            #     continue
            for balance_node in balance_nodes:
                # balance_node_code = balancenode.get_hour_balance_node(hour)
                # print('balance node %r' % balance_node.node_code)
                nodes_init = [balance_node]
                # cntr = 0
                while nodes_init:
                    # print('iteration %i' % cntr)
                    # for node in nodes_init:
                        # if node.get_node_hour_state(hour):
                        # print('node init %i' % (node.node_code))

                    next_lines = []
                    for node in nodes_init:
                        lines = lines_list.get_lines_by_node_from(node.node_code) + lines_list.get_lines_by_node_to(node.node_code)
                        next_lines += [l for l in lines if l.get_line_hour_state(hour)]

                    nodes = list(map(lambda l: l.node_from, next_lines)) + list(map(lambda l: l.node_to, next_lines))
                    next_nodes = []
                    for n in nodes:
                        if n is None:
                            continue
                        if n.get_node_hour_state(hour) and not n.get_hour_balance_node(hour) and n not in next_nodes:
                            next_nodes.append(n)
                        # next_nodes = [n for n in nodes if n.get_node_hour_state(hour) and not n.get_hour_balance_node(hour)]

                    # for line in next_lines:
                        # print('%i - %i' % (line.node_from_code, line.node_to_code))
                        # next_nodes += [n for n in nodes if n.get_node_hour_state(hour) and not n.get_hour_balance_node(hour)]
                    for node in next_nodes:
                        # if node.get_node_hour_state(hour):
                        # print('node %i' % (node.node_code))
                        node.set_hour_balance_node(hour, balance_node.node_code)
                    nodes_init = next_nodes
                    # cntr += 1



                # map(lambda n: n.set_hour_balance_node(hour, balance_node_code), next_nodes)


    def locate_balance_nodes(self):
        for hour, nodes in enumerate(Node.balance_nodes):
            for node_code in nodes:
                node = self[node_code]
                node.set_hour_balance_node(hour, node_code)
                self.balance_nodes[hour].append(node)



class Node(object):
    siberia_balance_node = [0]*HOURCOUNT
    balance_nodes = [[] for hour in range(HOURCOUNT)]

    def __init__(self, ns_row):
        self.hour_data = {}  # [0] * HOURCOUNT
        self.node_code = ns_row[ns['node_code']]
        self.area = ns_row[ns['area']]
        self.nominal_voltage = ns_row[ns['nominal_voltage']]
        self.price_zone = ns_row[ns['price_zone']]
        self.min_voltage = ns_row[ns['min_voltage']]
        self.max_voltage = ns_row[ns['max_voltage']]
        self.eq_db_nodes_data = []
        self.voltage_class = self.get_voltage_class(self.nominal_voltage)
        self.impex_data = []
        self.shunt_data = []
        self.pdem = [0 for hour in range(HOURCOUNT)]

    def turn_on_hour(self, hour):
        self.hour_data[hour].turn_on()

    def set_hour_balance_node(self, hour, node_code):
        self.hour_data[hour].set_balance_node(node_code)

    def get_hour_balance_node(self, hour):
        return self.hour_data[hour].balance_node_code

    def set_k_distr(self, hour, value):
        self.hour_data[hour].set_k_distr(value)

    @staticmethod
    def get_voltage_class(v):
        margin = 0.2
        voltage_classes = [2.8, 4.3, 6.4, 9.6, 14.4, 21.6, 32.4, 48.7, 73., 110.,
                           150., 220., 330., 500., 750., 1150.]
        for cl in voltage_classes:
            if abs(v / cl - 1) < margin:
                return cl
        return v

    @staticmethod
    def set_siberia_balance_node(hour, node_code):
        Node.siberia_balance_node[hour] = node_code

    @staticmethod
    def set_impex_area(impex_area):
        Node.impex_area = [i[ias['area']] for i in impex_area if i[ias['area']]]

    @staticmethod
    def get_siberia_balance_node(hour):
        return Node.siberia_balance_node[hour]

    def __str__(self):
        return'%i hours: %i' % (self.node_code, len(self.hour_data))

    def _check_hour_data(self, ns_row):
        for d in ['area', 'nominal_voltage', 'price_zone', 'min_voltage', 'max_voltage']:
            self._check_datum(d, ns_row[ns[d]])

    def add_to_pdem(self, hour, value):
        self.hour_data[hour].add_to_pdem(value)

    def add_to_retail(self, hour, value):
        self.hour_data[hour].add_to_retail(value)

    def add_node_hour_data(self, ns_row):
        hour = ns_row[ns['hour']]
        if hour in self.hour_data.keys():
            raise Exception('tried to insert one chunk of node hour data twice for node %i!' % self.node_code)
        self._check_hour_data(ns_row)
        hd = NodeHourData(ns_row)
        self.hour_data[hour] = hd
        # проверка, не балансирующий ли это узел в час hour
        if hd.is_balance_node() and hd.is_node_on():
            self.balance_nodes[hour].append(self.node_code)
        # заполнение псевдо-балансирующего узла Сибири
        if self.node_code in settings.siberia_balance_nodes:
            if not self.get_siberia_balance_node(hour):
                self.set_siberia_balance_node(hour, self.node_code)
            else:
                cur_index = settings.siberia_balance_nodes.index(self.get_siberia_balance_node(hour))
                index = settings.siberia_balance_nodes.index(self.node_code)
                if index < cur_index:
                    self.set_siberia_balance_node(hour, self.node_code)

    def _check_datum(self, attr, datum):
        if getattr(self, attr) != datum:
            raise Exception('%s not consistent for node %i' % (attr, self.node_code))

    def get_eq_db_nodes_data(self):
        if not self.eq_db_nodes_data:
            self.prepare_eq_db_nodes_data()
        return self.eq_db_nodes_data

    def get_impex_data(self):
        return self.impex_data

    def get_shunt_data(self):
        if not self.shunt_data:
            self.prepare_shunts_data()
        return self.shunt_data

    def get_node_hour_type(self, hour):
        return self.hour_data[hour].type

    def get_node_hour_load(self, hour):
        return self.hour_data[hour].pn

    def get_node_hour_state(self, hour):
        return self.hour_data[hour].is_node_on()

    def prepare_data(self):
        self.prepare_eq_db_nodes_data()
        self.prepare_impex_data()

    def prepare_eq_db_nodes_data(self):
        for hour, h in self.hour_data.items():
            if h.is_node_on():
                voltage, phase = h.get_eq_db_node_hour()
                price_zone = self.price_zone if self.price_zone else 1 if self.node_code < 1000000 else 2
                if h.is_balance_node():  # балансирующий узел
                    price_zone_fixed = price_zone
                elif self.node_code == self.get_siberia_balance_node(hour) or \
                        self.area in settings.fixed_siberia_areas:
                        # self.node_code in settings.fixed_siberia_nodes:
                    price_zone_fixed = 2
                elif self.area in settings.fixed_europe_areas:
                    price_zone_fixed = 1
                else:
                    price_zone_fixed = -1
                self.eq_db_nodes_data.append((
                    hour, self.node_code, self.voltage_class, voltage, phase,
                    self.nominal_voltage, self.area, price_zone, price_zone_fixed
                ))

    def prepare_impex_data(self):
        if self.area in Node.impex_area:
            for hour, h in self.hour_data.items():
                if h.is_node_on():
                    if h.type == 1:
                        volume = h.pn - h.pg
                    elif not h.is_balance_node():
                        volume = h.pg - h.pn
                    if volume:
                        self.impex_data.append((
                            hour, self.node_code, volume
                        ))

    def prepare_shunts_data(self):
        for hour, h in self.hour_data.items():
            if h.is_node_on() and (h.b_shr or h.g_shunt):
                self.shunt_data.append((
                    hour, self.node_code, self.voltage_class, h.g_shunt, -h.b_shr
                ))
