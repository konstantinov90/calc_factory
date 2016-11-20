import time
from utils import DB
from sql_scripts import StationsScript

ss = StationsScript()


def make_stations(tsid={}, tdate=''):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}
    print('making stations%s' % ((' for date %s' % tdate) if tdate else ''))
    start_time = time.time()

    con = DB.OracleConnection()

    stations = StationsList()

    @DB.process_cursor(con, ss, tsid)
    def process_stations(new_row, stations_list):
        stations_list.add_station(new_row)

    process_stations(stations)

    print('%s %s seconds %s' % (15 * '-',  round(time.time() - start_time, 3), 15 * '-'))

    return stations


class StationsList(object):
    def __init__(self):
        self.stations_list = []
        self.stations_list_index = {}

    def __len__(self):
        return len(self.stations_list)

    def __iter__(self):
        for s in self.stations_list:
            yield(s)

    def __getitem__(self, item):
        if item in self.stations_list_index.keys():
            return self.stations_list[self.stations_list_index[item]]
        else:
            return None

    def add_station(self, ss_row):
        station_id = ss_row[ss['id']]
        self.stations_list_index[station_id] = len(self.stations_list)
        self.stations_list.append(Station(ss_row))


class Station(object):
    def __init__(self, ss_row):
        self.id = ss_row[ss['id']]
        self.code = ss_row[ss['code']]
        self.type = ss_row[ss['type']]
        self.category = ss_row[ss['category']]
