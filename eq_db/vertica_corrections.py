import time
import decimal
from utils import DB

from sql_scripts import StationsScript_V
from sql_scripts import DGUsScript_V
from sql_scripts import RastrGenScript_V
from sql_scripts import GeneratorsScript_V
from sql_scripts import GUsScript_V
from sql_scripts import NBlockScript_V
from sql_scripts import NodesScript_V
from sql_scripts import LinesScript_V
from sql_scripts import BidInitsScript_V
from sql_scripts import BidHoursScript_V
from sql_scripts import BidPairsScript_V

HOURCOUNT = 24


bid_init_s = BidInitsScript_V()
bid_hours_s = BidHoursScript_V()
bid_pairs_s = BidPairsScript_V()

def add_bids_vertica(bids, scenario, tdate):
    scenario = {'scenario': scenario}
    print('adding bids from vertica for date %s' % tdate)
    start_time = time.time()

    con = DB.VerticaConnection()

    @DB.process_cursor(con, bid_init_s, scenario)
    def add_bids(new_row, bids_list):
        bids_list.add_bid(new_row)

    @DB.process_cursor(con, bid_hours_s, scenario)
    def add_bid_hours_data(new_row, bids_list):
        dpg_id = new_row[bid_hours_s['dpg_id']]
        bid = bids_list[dpg_id]
        if bid:
            bid.add_hour_data(new_row)

    @DB.process_cursor(con, bid_pairs_s, scenario)
    def add_bid_pairs_data(new_row, bids_list):
        dpg_id = new_row[bid_pairs_s['dpg_id']]
        bid = bids_list[dpg_id]
        if bid:
            bid.add_intervals_data(new_row)

    add_bids(bids)
    add_bid_hours_data(bids)
    add_bid_pairs_data(bids)

    print('%s %s seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))

    return bids


lines_s_v = LinesScript_V()

def add_lines_vertica(lines, scenario, tdate):
    scenario = {'scenario': scenario}
    print('adding lines from vertica for date %s' % tdate)
    start_time = time.time()

    con = DB.VerticaConnection()

    @DB.process_cursor(con, lines_s_v, scenario)
    def add_lines(new_row, lines_list):
        node_from_code = new_row[lines_s_v['node_from']]
        node_to_code = new_row[lines_s_v['node_to']]
        line_par_num = new_row[lines_s_v['n_par']]
        if not lines_list.get_line(node_from_code, node_to_code, line_par_num):

            lines_list.add_line(new_row)
            line = lines_list.get_line(node_from_code, node_to_code, line_par_num)
            for hour in range(HOURCOUNT):
                row = new_row[:lines_s_v['hour']] + [hour] + new_row[(lines_s_v['hour'] + 1):]
                line.add_line_hour_data(row)

    add_lines(lines)

    print('%s %s seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))

    return lines


nodes_s_v = NodesScript_V()

def add_nodes_vertica(nodes, scenario, tdate):
    scenario = {'scenario': scenario}
    print('adding nodes from vertica for date %s' % tdate)
    start_time = time.time()

    con = DB.VerticaConnection()

    @DB.process_cursor(con, nodes_s_v, scenario)
    def add_nodes(new_row, node_list):
        node_code = new_row[nodes_s_v['node_code']]
        node_list.add_node(new_row)
        for hour in range(HOURCOUNT):
            row = new_row[:nodes_s_v['hour']] + [hour] + new_row[(nodes_s_v['hour'] + 1):]
            node_list[node_code].add_node_hour_data(row)

    add_nodes(nodes)

    print('%s %s seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))

    return nodes


gens_script_v = GeneratorsScript_V()

def add_supplies_vertica(dpgs, scenario, tdate):
    scenario = {'scenario': scenario}
    print('adding supplies from vertica for date %s' % tdate)
    start_time = time.time()

    con = DB.VerticaConnection()

    @DB.process_cursor(con, gens_script_v, scenario)
    def add_supplies(new_row, dpgs_list):
        dpg_id = new_row[gens_script_v['gtp_id']]
        dpgs_list.add_generator(new_row)
        dpgs_list[dpg_id].attach_to_fed_station(dpgs)

    add_supplies(dpgs)

    print('%s %s seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))

    return dpgs


NEWDGU = True
dgs_v = DGUsScript_V()
rgs_v = RastrGenScript_V()

def add_dgus_vertica(dgus, scenario, tdate):
    scenario = {'scenario': scenario}
    print('adding dgus from vertica for date %s' % tdate)
    start_time = time.time()

    con = DB.VerticaConnection()

    @DB.process_cursor(con, dgs_v, scenario)
    def add_dgus(new_row, dgus_list):
        dgus_list.add_dgu(new_row, NEWDGU)

    @DB.process_cursor(con, rgs_v, scenario)
    def add_dgus_data(new_row, dgus_list):
        dgu_code = new_row[rgs_v['rge_code']]
        dgu = dgus_list.get_dgu_by_code(dgu_code)
        if dgu:
            dgu.add_dgu_hour_data(new_row)
            # [(int(n) if int(n) == n else float(n)) if isinstance(n, decimal.Decimal) else n for n in new_row]

    add_dgus(dgus)
    add_dgus_data(dgus)

    print('%s %s seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))

    return dgus


gs_v = GUsScript_V()
ns_v = NBlockScript_V()

def add_gus_vertica(gus, scenario, tdate):
    scenario = {'scenario': scenario}
    print('adding gus from vertica for date %s' % tdate)
    start_time = time.time()

    con = DB.VerticaConnection()

    @DB.process_cursor(con, gs_v, scenario)
    def add_gus(new_row, gus_list):
        # print('added %i' % new_row[gs_v['code']])
        gus_list.add_gu_from_rio(new_row)

    @DB.process_cursor(con, ns_v, scenario)
    def add_nblock_data(new_row, gus_list):
        gu_code = new_row[ns_v['gu_code']]
        gus = gus_list[gu_code]
        if gus:
            for gu in gus:
                gu.add_gu_hour_data(new_row)

    @DB.process_cursor(con, gs_v, scenario)
    def remove_gus(new_row, gus_list):
        gu_code = new_row[gs_v['code']]
        # try:
        gus_list.set_to_remove(gu_code)
        # except Exception:
        #     rai

    scenario['inout'] = 1
    add_gus(gus)
    add_nblock_data(gus)

    scenario['inout'] = 0
    remove_gus(gus)

    print('%s %s seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))

    return gus


ss_v = StationsScript_V()

def add_stations_vertica(stations, scenario, tdate):
    scenario = {'scenario': scenario}
    print('adding stations from vertica for date %s' % tdate)
    start_time = time.time()

    con = DB.VerticaConnection()
    cntr = 0

    @DB.process_cursor(con, ss_v, scenario)
    def add_stations(new_row, stations_list, cntr):
        station_id = new_row[ss_v['id']]
        if not stations_list[station_id]:
            cntr += 1
            stations_list.add_station(new_row)

    add_stations(stations, cntr)

    # print('added %i stations from vertica' % cntr)
    print('%s %s seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))

    return stations
