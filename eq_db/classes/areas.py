import time
from utils import DB
from sql_scripts import RastrAreaScript
from eq_db.classes.nodes import NodesList


HOURCOUNT = 24
ra = RastrAreaScript()


def make_areas(tsid={}, tdate=''):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}
    print('making areas%s' % ((' for date %s' % tdate) if tdate else ''))
    start_time = time.time()

    con = DB.OracleConnection()

    areas = AreasList()

    @DB.process_cursor(con, ra, tsid)
    def process_areas(new_row, areas_list):
        code = new_row[ra['area']]
        area = areas_list[code]
        if not area:
            areas_list.add_area(new_row)
        areas_list[code].add_area_hour_data(new_row)

    process_areas(areas)

    print('%s %s seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))

    return areas


class AreasList(object):
    def __init__(self):
        self.areas_list = []
        self.areas_list_index = {}

    def __len__(self):
        return len(self.areas_list)

    def __iter__(self):
        for a in self.areas_list:
            yield a

    def __getitem__(self, item):
        if item in self.areas_list_index.keys():
            return self.areas_list[self.areas_list_index[item]]
        else:
            return None

    def add_area(self, ra_row):
        area = ra_row[ra['area']]
        self.areas_list_index[area] = len(self.areas_list)
        self.areas_list.append(Area(ra_row))

    def attach_nodes(self, nodes_list):
        for a in self.areas_list:
            a.attach_nodes(nodes_list)


class AreaHourData(object):
    def __init__(self, ra_row):
        self.losses = ra_row[ra['losses']]
        self.load_losses = ra_row[ra['load_losses']]


class Area(object):
    def __init__(self, ra_row):
        self.code = ra_row[ra['area']]
        self.area_hour_data = {}
        self.nodes = NodesList()
        self.sum_pn_retail_diff = [0 for hour in range(HOURCOUNT)]
        self.nodes_on = [0 for hour in range(HOURCOUNT)]

    def add_area_hour_data(self, ra_row):
        hour = ra_row[ra['hour']]
        if hour in self.area_hour_data.keys():
            raise Exception('tried to add area %i hour twice!' % self.code)
        self.area_hour_data[hour] = AreaHourData(ra_row)

    def attach_nodes(self, nodes_list):
        for node in nodes_list.get_nodes_by_area(self.code):
            self.nodes.append(node)
            for hour, hd in node.hour_data.items():
                self.sum_pn_retail_diff[hour] += max(hd.pn - hd.retail, 0) * hd.is_node_on()
                self.nodes_on[hour] += hd.is_node_on()
        for node in self.nodes:
            for hour, hd in node.hour_data.items():

                k_distr = (max(hd.pn - hd.retail, 0) * hd.is_node_on() / self.sum_pn_retail_diff[hour]) if self.sum_pn_retail_diff[hour] else ((hd.is_node_on() / self.nodes_on[hour]) if self.nodes_on[hour] else 0)
                node.set_k_distr(hour, k_distr)
