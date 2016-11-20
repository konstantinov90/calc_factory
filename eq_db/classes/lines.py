import math
import time
from operator import itemgetter
from sql_scripts import LinesScript
from utils import DB
from utils.progress_bar import update_progress

HOURCOUNT = 24

ls = LinesScript()


def make_lines(tsid={}, tdate=''):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}
    # if not node_list:
    #     node_list = make_nodes(tsid, tdate)
    print('making lines%s' % ((' for date %s' % tdate) if tdate else ''))

    start_time = time.time()

    con = DB.OracleConnection()

    lines = LinesList()

    @DB.process_cursor(con, ls, tsid)
    def process_lines(new_row, line_list):
        node_from_code = new_row[ls['node_from']]
        node_to_code = new_row[ls['node_to']]
        line_par_num = new_row[ls['n_par']]
        line = line_list.get_line(node_from_code, node_to_code, line_par_num)
        if not line:
            line = line_list.add_line(new_row)
        line.add_line_hour_data(new_row)

    # print('loading lines information')
    process_lines(lines)

    # for i, line in enumerate(lines):
    #     line.attach_nodes(node_list)
    #     update_progress((i + 1) / len(lines))

    # [print(n) for n in nodes[nodes_index[1001068]].get_eq_db_nodes_data()]

    print('%s %i seconds %s' % (15 * '-', time.time() - start_time, 15 * '-'))

    return lines


class LinesList(object):
    def __init__(self):
        self.lines_list = []
        self.lines_index = {}
        self.lines_data = [[] for hour in range(HOURCOUNT)]
        self.lines_index_by_node_from = {}
        self.lines_index_by_node_to = {}

    def attach_nodes(self, nodes_list):
        for line in self.lines_list:
            line.attach_nodes(nodes_list)

    def prepare_lines_data(self):
        l_s = {'hour': 0, 'node_from_code': 0, 'node_to_code': 1, 'parallel_num': 2}
        h_ind = l_s['hour']
        for line in self.lines_list:
            [self.lines_data[d[h_ind]].append(d[:h_ind] + d[(1 + h_ind):])
                for d in line.get_prepared_lines_data()]
        [l.sort(key=itemgetter(l_s['node_from_code'], l_s['node_to_code'], l_s['parallel_num'])) for l in self.lines_data]


    def get_prepared_lines_data(self):
        if not self.lines_data[0]:
            self.prepare_lines_data()
        return self.lines_data

    def __iter__(self):
        for l in self.lines_list:
            yield l

    def __len__(self):
        return len(self.lines_list)

    def add_line(self, ls_row):
        node_from_code = ls_row[ls['node_from']]
        node_to_code = ls_row[ls['node_to']]
        line_par_num = ls_row[ls['n_par']]
        cur_num = len(self.lines_list)

        if not node_from_code in self.lines_index_by_node_from.keys():
            self.lines_index_by_node_from[node_from_code] = []
        self.lines_index_by_node_from[node_from_code].append(cur_num)

        if not node_to_code in self.lines_index_by_node_to.keys():
            self.lines_index_by_node_to[node_to_code] = []
        self.lines_index_by_node_to[node_to_code].append(cur_num)

        if node_from_code in self.lines_index.keys():
            if node_to_code in self.lines_index[node_from_code].keys():
                if line_par_num in self.lines_index[node_from_code][node_to_code].keys():
                    raise Exception('tried to add same line %i - %i n_par = %i twice!'
                                    % (node_from_code, node_to_code, line_par_num))
                else:
                    self.lines_index[node_from_code][node_to_code][line_par_num] = cur_num
            else:
                self.lines_index[node_from_code][node_to_code] = {line_par_num: cur_num}
        else:
            self.lines_index[node_from_code] = {node_to_code: {line_par_num: cur_num}}
        new_line = Line(ls_row)
        self.lines_list.append(new_line)
        return new_line

    def get_lines_by_node_from(self, item):
        ret = []
        if item in self.lines_index_by_node_from.keys():
            for idx in self.lines_index_by_node_from[item]:
                ret.append(self.lines_list[idx])
        return ret

    def get_lines_by_node_to(self, item):
        ret = []
        if item in self.lines_index_by_node_to.keys():
            for idx in self.lines_index_by_node_to[item]:
                ret.append(self.lines_list[idx])
        return ret

    def get_line(self, node_from_code, node_to_code, num_par=None):
        if node_from_code in self.lines_index.keys():
            if node_to_code in self.lines_index[node_from_code].keys():
                # если указан номер параллели - возвращаем эту ветку
                # иначе возвращаем подсписок всех параллелей
                if num_par is not None:
                    if num_par in self.lines_index[node_from_code][node_to_code].keys():
                        return self.lines_list[self.lines_index[node_from_code][node_to_code][num_par]]
                    else:
                        return None
                else:
                    return [self.lines_list[self.lines_index[node_from_code][node_to_code][n]]
                            for n in self.lines_index[node_from_code][node_to_code].keys()]

            else:
                return None
        else:
            return None


class LineHourData(object):
    def __init__(self, ls_row):
        self.r = ls_row[ls['r']]
        self.x = ls_row[ls['x']]
        self.b = ls_row[ls['b']]
        self.g = ls_row[ls['g']]
        self.b_from = ls_row[ls['b_from']]
        self.b_to = ls_row[ls['b_to']]
        self.losses = ls_row[ls['losses']]
        self.state = ls_row[ls['state']]

    def is_line_on(self):
        return not self.state


class Line(object):
    def __init__(self, ls_row):
        self.node_from_code = ls_row[ls['node_from']]
        self.node_from = None
        self.node_to_code = ls_row[ls['node_to']]
        self.node_to = None
        self.parallel_num = ls_row[ls['n_par']]
        self.kt_re = ls_row[ls['kt_re']]
        self.kt_im = ls_row[ls['kt_im']]
        self.div = ls_row[ls['div']]
        self.type = ls_row[ls['type']]
        self.area = ls_row[ls['area']]
        self.hour_data = [0] * HOURCOUNT
        self.prepared_lines_data = []
        self.group_line_div = {}
        self.group_line_flipped = {}

    def __str__(self):
        return '%i - %i npar = %i' % (self.node_from_code, self.node_to_code, self.parallel_num)

    def attach_nodes(self, nodes_list):
        self.node_from = nodes_list[self.node_from_code]
        self.node_to = nodes_list[self.node_to_code]

    def set_group_line_div(self, section_code, div):
        self.group_line_div[section_code] = div

    def set_group_flipped(self, section_code, flipped):
        self.group_line_flipped[section_code] = flipped

    def get_line_hour_state(self, hour):
        return self.hour_data[hour].is_line_on()

    def add_line_hour_data(self, ls_row):
        self._check_hour_data(ls_row)
        hour = ls_row[ls['hour']]
        self.hour_data[hour] = LineHourData(ls_row)

    def _check_hour_data(self, ls_row):
        for d in ['kt_re', 'kt_im', 'div', 'type', 'area']:
            self._check_datum(d, ls_row[ls[d]])

    def _check_datum(self, attr, datum):
        if getattr(self, attr) != datum:
            raise Exception('%s not consistent for line %i - %i n_par = %i'
                            % (attr, self.node_code_from, self.node_code_to, self.parallel_num))

    def get_prepared_lines_data(self):
        if not self.prepared_lines_data:
            self.prepare_lines_data()
        return self.prepared_lines_data

    def prepare_lines_data(self):
        for hour, lh in enumerate(self.hour_data):
            try:
                if lh.is_line_on() and self.node_from.get_node_hour_state(hour) and self.node_to.get_node_hour_state(hour):
                    if not self.type:
                        node_start = self.node_from_code
                        node_finish = self.node_to_code
                        base_coeff = 0
                        k_pu = 0
                    else:
                        node_start = self.node_to_code
                        node_finish = self.node_from_code
                        base_coeff = self.node_to.voltage_class / self.node_from.voltage_class
                        k_pu = math.sqrt(math.pow(self.kt_re, 2) + math.pow(self.kt_im, 2))
                    lag = math.atan(self.kt_im / self.kt_re) if self.kt_re else 0

                    self.prepared_lines_data.append((
                        hour, node_start, node_finish, self.parallel_num, self.type,
                        max(self.node_from.voltage_class, self.node_to.voltage_class), base_coeff,
                        lh.r, lh.x, lh.g, -lh.b, k_pu, lag, -lh.b_from, -lh.b_to
                    ))
            except Exception:
                print('ERROR! line %i-%i has no node(s)' % (self.node_from_code, self.node_to_code))
