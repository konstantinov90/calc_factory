import math
import time
from operator import itemgetter
from sql_scripts import lines_script as ls
from utils import DB, ORM
from utils.progress_bar import update_progress
from .lines import Line
from .lines import LineHourData
from sqlalchemy.orm import relationship
from sqlalchemy import exists

HOURCOUNT = 24

# Line.hours = relationship('LineHourData')
# LineHourData.line_id = relationship('Line', back_populates)
# LineHourData.line = relationship('Line', back_populates='hours')

# LineHourData.line = relationship('Line')


def make_lines(tsid={}, tdate=''):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}
    # if not node_list:
    #     node_list = make_nodes(tsid, tdate)
    print('making lines%s' % ((' for date %s' % tdate) if tdate else ''))

    start_time = time.time()

    con = DB.OracleConnection()

    # ORM.recreate_all()

    lines = LinesList()

    @DB.process_cursor(con, ls, tsid)
    def process_lines(new_row, line_list):
        node_from_code = new_row[ls['node_from']]
        node_to_code = new_row[ls['node_to']]
        line_par_num = new_row[ls['n_par']]
        # # print('%i - %i: %i' % (node_from_code, node_to_code, line_par_num))
        # if not ORM.session.query(exists().where((Line.node_from_code == node_from_code) & (Line.node_to_code == node_to_code) & (Line.parallel_num == line_par_num))).scalar():
        #     ORM.session.add(Line(new_row))
        #     ORM.session.flush()
        #     # print('added')
        # line = ORM.session.query(Line).filter_by(node_from_code=node_from_code, node_to_code=node_to_code, parallel_num=line_par_num).one()
        # # print('%i - %i: %i (%i)' % (line.node_from_code, line.node_to_code, line.parallel_num, line.id))
        # l_hd = LineHourData(new_row)
        # l_hd.line_id = line.id
        # ORM.session.add(l_hd)
        # ORM.session.commit()
        line = line_list.get_line(node_from_code, node_to_code, line_par_num)
        if not line:
            line = line_list.add_line(new_row)
        line.add_line_hour_data(new_row)

    # print('loading lines information')
    process_lines(lines)
    # ORM.session.add_all(lines.lines_list)
    # ORM.session.commit()

    for i, line in enumerate(lines):
    #     for hd in line.hour_data:
    #         hd.line_id = line.id
        line.serialize(ORM.session)
        update_progress((i + 1) / len(lines))
    ORM.session.commit()
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
