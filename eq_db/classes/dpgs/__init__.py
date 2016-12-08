import time
import itertools
from operator import itemgetter

from utils import DB, ORM
from utils.progress_bar import update_progress
from sql_scripts import consumers_script as cs
from sql_scripts import generators_script as gs
from sql_scripts import impex_dpgs_script as imp_s
# from sql_scripts import MaxBidPriceScript
from sql_scripts import disqualified_data_script as dds
# from sql_scripts import wsumgen_script as ws
# from sql_scripts import GeneratorsLastHourScript

from .dpg_demand_fsk import DpgDemandFSK
from .dpg_demand_system import DpgDemandSystem
from .dpg_demand_load import DpgDemandLoad
from .dpg_demand import DpgDemand
from .dpg_supply import DpgSupply
from .dpg_impex import DpgImpex
from .dpg_disqualified import DpgDisqualified

HOURCOUNT = 24

# glhs = GeneratorsLastHourScript()
# mbs = MaxBidPriceScript()

dpg_consumer_bid_index = {'hour': 0, 'node_code': 3, 'volume': 4}
dpg_fixedcon_index = {'hour': 0, 'node_code': 1, 'volume': 3}


def make_dpgs(tsid={}, tdate=''):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}
    print('making dpgs%s' % ((' for date %s' % tdate) if tdate else ''))

    start_time = time.time()

    con = DB.OracleConnection()

    @DB.process_cursor(con, cs, tsid)
    def process_consumers(new_row, dpg_list):
        dpg_list.add_consumer(new_row)

    @DB.process_cursor(con, dds, tsid)
    def process_disqualified(new_row, dpg_list):
        dpg_id = new_row[dds['dpg_id']]
        dpg_list[dpg_id].add_disqualified_data(new_row)

    @DB.process_cursor(con, gs, tsid)
    def process_generators(new_row, dpg_list):
        dpg_list.add_generator(new_row)

    @DB.process_cursor(con, imp_s, tsid)
    def process_impex_dpgs(new_row, dpg_list):
        dpg_list.add_impex(new_row)

    # DpgDemandLoad.set_max_bid_prices(con.exec_script(mbs.get_query(), tsid))
    # DpgDemandLoad.add_disqualified_data(con.exec_script(dds.get_query(), tsid))
    # DpgSupply.set_wsum_data(con.exec_script(ws.get_query(), tsid))
    # DpgSupply.set_last_hour_data(con.exec_script(glhs.get_query(), tsid))

    dpgs = DpgList()
    process_consumers(dpgs)
    process_generators(dpgs)
    process_impex_dpgs(dpgs)
    process_disqualified(dpgs)

    # dpgs.finalize_data()

    # ORM.session.add_all(dpgs.dpg_list)
    for i, dpg in enumerate(dpgs):
        dpg.serialize(ORM.session)
        update_progress((i + 1) / len(dpgs))
    ORM.session.commit()
    for i, dpg in enumerate(DpgDemandLoad):
        dpg.recalculate()
        update_progress((i + 1) / len(dpgs))
    for i, dpg in enumerate(DpgDemandSystem):
        dpg.recalculate()
        update_progress((i + 1) / len(dpgs))
    ORM.session.commit()


    print('%s %s seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))

    return dpgs


class DpgList(object):
    def __init__(self):
        self.dpg_list = []
        self.dpg_list_index = {}
        self.prepared_supplies_data = [[] for hour in range(HOURCOUNT)]
        self.prepared_demands_data = [[] for hour in range(HOURCOUNT)]
        self.prepared_impex_bids_data = [[] for hour in range(HOURCOUNT)]
        self.prepared_fuel_data = []
        self.prepared_price_zone_consumption = [[] for hour in range(HOURCOUNT)]
        self.prepared_generator_last_hour_data = []
        self.hours = [(hour + 1,) for hour in range(HOURCOUNT)]

    # def finalize_data(self):
    #     for i, dpg in enumerate(self.dpg_list):
    #         dpg.attach_to_fed_station(self)
    #         update_progress((i + 1) / len(self.dpg_list))

    def set_areas(self, areas_list):
        for dpg in self.dpg_list:
            dpg.set_area(areas_list)

    def set_loads(self, loads_list):
        for dpg in self.dpg_list:
            dpg.set_load(loads_list)

    def set_consumers(self, consumers_list):
        for dpg in self.dpg_list:
            dpg.set_consumer(consumers_list)

    def attach_sections(self, sections_list):
        for dpg in self.dpg_list:
            dpg.attach_sections(sections_list)

    def set_bids(self, bids_list):
        for dpg in self.dpg_list:
            dpg.set_bid(bids_list[dpg.id])

    def set_stations(self, stations_list):
        for dpg in self.dpg_list:
            dpg.set_station(stations_list[dpg.station_id])

    def get_hours_data(self):
        return self.hours

    def get_prepared_price_zone_consumption(self):
        if not self.prepared_price_zone_consumption[0]:
            self.prepare_price_zone_consumption()
        return self.prepared_price_zone_consumption

    def prepare_price_zone_consumption(self):
        index = {}
        for dpg in self.dpg_list:
            if not isinstance(dpg, DpgDemand):
                continue
            for i, bid in enumerate(itertools.chain(dpg.get_distributed_bid(), dpg.get_fixedcon_data())):
                if i < len(dpg.get_distributed_bid()):
                    bid_index = dpg_consumer_bid_index
                else:
                    bid_index = dpg_fixedcon_index

                hour = bid[bid_index['hour']]
                if not hour in index.keys():
                    index[hour] = {}
                price_zone = 2 if bid[bid_index['node_code']] > 999999 else 1
                if price_zone not in index[hour].keys():
                    i = len(self.prepared_price_zone_consumption[hour])
                    index[hour][price_zone] = i
                    self.prepared_price_zone_consumption[hour].append([
                        price_zone, 0
                    ])
                i = index[hour][price_zone]
                self.prepared_price_zone_consumption[hour][i][1] += abs(bid[bid_index['volume']])
        [l.sort(key=itemgetter(0)) for l in self.prepared_price_zone_consumption]

    def get_prepared_fuel_data(self):
        if not self.prepared_fuel_data:
            self.prepare_fuel_data()
        return self.prepared_fuel_data

    def prepare_fuel_data(self):
        for row in DpgSupply.get_wsumgen_data():
            self.prepared_fuel_data.append((
                row[ws['integral_id']], row[ws['rge_code']], float(row[ws['min_volume']]),
                row[ws['volume']], row[ws['hour_start']], row[ws['hour_end']]
            ))

    def get_prepared_generator_last_hour_data(self):
        if not self.prepared_generator_last_hour_data:
            self.prepare_generator_last_hour_data()
        return self.prepared_generator_last_hour_data

    def prepare_generator_last_hour_data(self):
        self.prepared_generator_last_hour_data = sorted(DpgSupply.get_last_hour_data(), key=itemgetter(glhs['rge_code']))

    def get_prepared_supplies_data(self):
        if not self.prepared_supplies_data[0]:
            self.prepare_data()
        return self.prepared_supplies_data

    def get_prepared_demands_data(self):
        if not self.prepared_demands_data[0]:
            self.prepare_data()
        return self.prepared_demands_data

    def get_prepared_impex_bids_data(self):
        if not self.prepared_impex_bids_data[0]:
            self.prepare_data()
        return self.prepared_impex_bids_data

    def prepare_data(self):
        s_int = {'hour': 0, 'rge_code': 4, 'interval_number': 5, 'node_code': 0}
        h_s_int = s_int['hour']
        d_int = {'hour': 0, 'interval_code': 0, 'interval_number': 2, 'node_code': 3, 'consumer_code': 1}
        h_d_int = d_int['hour']
        i_int = {'hour': 0, 'section_number': 0, 'interval_number': 2}
        h_i_int = i_int['hour']

        demand_ids = {}
        fict_dpg_list = sorted(self.dpg_list, key = lambda dpg: dpg.consumer_code if isinstance(dpg, DpgDemandLoad) or isinstance(dpg, DpgDemandSystem) else 0)

        interval_code = [0 for hour in range(HOURCOUNT)]

        for dpg in fict_dpg_list:
            if isinstance(dpg, DpgDemand):
                distributed_bid = dpg.get_distributed_bid()
                prev_interval_number = [None for hour in range(HOURCOUNT)]
                # prev_hour = None
                for d in sorted(distributed_bid, key=itemgetter(d_int['hour'], d_int['interval_number'])):
                    hour = d[h_d_int]
                    # if prev_interval_number[hour] and prev_hour and (d[d_int['interval_number']] != prev_interval_number[hour] or hour != prev_hour):
                    if prev_interval_number[hour] is None or d[d_int['interval_number']] != prev_interval_number[hour]:
                        interval_code[hour] += 1
                    prev_interval_number[hour] = d[d_int['interval_number']]
                    # prev_hour = hour

                    self.prepared_demands_data[hour].append(
                        d[:h_d_int] + (interval_code[hour],) + d[(h_d_int + 1):]
                    )

            elif isinstance(dpg, DpgSupply):
                [self.prepared_supplies_data[d[h_s_int]].append(d[:h_s_int] + d[(h_s_int + 1):])
                    for d in dpg.get_distributed_bid()]
            elif isinstance(dpg, DpgImpex):
                [self.prepared_impex_bids_data[d[h_s_int]].append(d[:h_i_int] + d[(h_i_int + 1):])
                    for d in dpg.get_distributed_bid()]

        [l.sort(key=itemgetter(s_int['rge_code'], s_int['interval_number'], s_int['node_code'])) for l in self.prepared_supplies_data]
        [l.sort(key=itemgetter(d_int['interval_code'], d_int['interval_number'], d_int['node_code'])) for l in self.prepared_demands_data]
        [l.sort(key=itemgetter(i_int['section_number'], i_int['interval_number'])) for l in self.prepared_impex_bids_data]

    def __iter__(self):
        for d in self.dpg_list:
            yield d

    def __len__(self):
        return len(self.dpg_list)

    def __getitem__(self, item):
        if item in self.dpg_list_index.keys():
            return self.dpg_list[self.dpg_list_index[item]]
        else:
            return None

    def add_consumer(self, cs_row):
        self.dpg_list_index[cs_row[cs['dpg_id']]] = len(self.dpg_list)

        if cs_row[cs['is_fsk']]:
            self.dpg_list.append(DpgDemandFSK(cs_row))
        elif cs_row[cs['is_system']]:
            self.dpg_list.append(DpgDemandSystem(cs_row))
        else:
            self.dpg_list.append(DpgDemandLoad(cs_row))

    def add_generator(self, gs_row):
        self.dpg_list_index[gs_row[gs['gtp_id']]] = len(self.dpg_list)
        self.dpg_list.append(DpgSupply(gs_row))

    def add_impex(self, imp_s_row):
        self.dpg_list_index[imp_s_row[imp_s['dpg_id']]] = len(self.dpg_list)
        self.dpg_list.append(DpgImpex(imp_s_row))

    def calculate_node_load(self):
        for d in self.dpg_list:
            d.calculate_node_load()

    def calculate_dpg_node_load(self):
        for d in self.dpg_list:
            d.calculate_dpg_node_load()
