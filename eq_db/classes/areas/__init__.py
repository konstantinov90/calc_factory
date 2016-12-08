import time
from utils import DB, ORM
from utils.progress_bar import update_progress
from sql_scripts import RastrAreaScript
from .areas import Area


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
    ORM.session.add_all(areas.areas_list)
    for i, area in enumerate(areas):
        ORM.session.add_all(area.area_hour_data.values())
        update_progress((i + 1) / len(areas))
    ORM.session.commit()


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
