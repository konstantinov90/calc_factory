"""Initialize and intertwine model."""
import time
from concurrent import futures

from utils import DB
from eq_db.classes.sections import make_sections, add_sections_vertica, Section
from eq_db.classes.dgu_groups import make_dgu_groups, DguGroup
from eq_db.classes.bids import make_bids, add_bids_vertica, send_bids_to_db, Bid
from eq_db.classes.stations import make_stations, add_stations_vertica, \
                                   send_stations_to_db, Station
from eq_db.classes.areas import make_areas, add_areas_vertica, send_areas_to_db, Area
from eq_db.classes.impex_areas import make_impex_areas, ImpexArea
from eq_db.classes.nodes import make_nodes, add_nodes_vertica, send_nodes_to_db, Node
from eq_db.classes.loads import make_loads, add_loads_vertica, send_loads_to_db, Load
from eq_db.classes.consumers import make_consumers, add_consumers_vertica, \
                                    send_consumers_to_db, Consumer
from eq_db.classes.dpgs import make_dpgs, add_dpgs_vertica, \
                               send_dpgs_to_db, send_dpgs_to_kc_dpg_node, \
                               send_dpgs_to_kg_dpg_rge, send_dpgs_to_node_bid_pair
from eq_db.classes.dpgs.base_dpg import Dpg
from eq_db.classes.dpgs.dpg_demand import DpgDemand
from eq_db.classes.dpgs.dpg_demand_fsk import DpgDemandFSK
from eq_db.classes.dpgs.dpg_demand_system import DpgDemandSystem
from eq_db.classes.dpgs.dpg_demand_load import DpgDemandLoad
from eq_db.classes.dpgs.dpg_supply import DpgSupply
from eq_db.classes.dpgs.dpg_impex import DpgImpex
from eq_db.classes.dgus import make_dgus, add_dgus_vertica, send_dgus_to_db, Dgu
from eq_db.classes.wsumgen import make_wsumgen, Wsumgen
from eq_db.classes.gus import make_gus, add_gus_vertica, Gu
from eq_db.classes.lines import make_lines, add_lines_vertica, send_lines_to_db, Line
from eq_db.classes.price_zone import make_price_zones, PriceZone
from eq_db.classes.settings import make_settings, Setting
from eq_db.classes.bids_max_prices import make_bid_max_prices, BidMaxPrice
from eq_db.classes.peak_so import make_peak_so, PeakSO

# tsid = 221076901
# scenario = 1
# tdate = '29-05-2015'
def initialize_model(tsid, target_date):
    """make instances of model classes"""
    start_time = time.time()
    tdate = target_date.strftime('%Y-%m-%d')

    # with futures.ThreadPoolExecutor(8) as executor:
    #     res = [executor.submit(fn, tsid, tdate=tdate) for fn in [make_sections,
    #                                                             make_dgu_groups,
    #                                                             make_bids,
    #                                                             make_stations,
    #                                                             make_areas,
    #                                                             make_impex_areas,
    #                                                             make_nodes,
    #                                                             make_loads,
    #                                                             make_consumers,
    #                                                             make_dpgs,
    #                                                             make_dgus,
    #                                                             make_wsumgen,
    #                                                             make_gus,
    #                                                             make_lines,
    #                                                             make_price_zones,
    #                                                             make_settings]]
    #
    # list(res)

    make_sections(tsid, tdate=tdate)
    make_dgu_groups(tsid, tdate=tdate)
    make_bids(tsid, tdate=tdate)
    make_stations(tsid, tdate=tdate)
    make_areas(tsid, tdate=tdate)
    make_impex_areas(tsid, tdate=tdate)
    make_nodes(tsid, tdate=tdate)
    make_loads(tsid, tdate=tdate)
    make_consumers(tsid, tdate=tdate)
    make_dpgs(tsid, tdate=tdate)
    make_dgus(tsid, tdate=tdate)
    make_wsumgen(tsid, tdate=tdate)
    make_gus(tsid, tdate=tdate)
    make_lines(tsid, tdate=tdate)
    make_price_zones(tsid, tdate=tdate)
    make_settings(tsid, tdate=tdate)
    make_peak_so(tsid, target_date, tdate=tdate)

    print(time.time() - start_time)

def augment_model(scenario, target_date):
    """add instances of model classes from vertica"""
    start_time = time.time()
    tdate = target_date.strftime('%Y-%m-%d')

    add_loads_vertica(scenario, tdate=tdate)
    add_areas_vertica(scenario, tdate=tdate)
    add_dpgs_vertica(scenario, tdate=tdate)
    add_consumers_vertica(scenario, tdate=tdate)
    add_sections_vertica(scenario, tdate=tdate)
    add_bids_vertica(scenario, tdate=tdate)
    add_stations_vertica(scenario, tdate=tdate)
    add_nodes_vertica(scenario, tdate=tdate)
    add_dgus_vertica(scenario, tdate=tdate)
    add_lines_vertica(scenario, tdate=tdate)
    add_gus_vertica(scenario, tdate=tdate)

    print(time.time() - start_time)

def intertwine_model():
    """set intramodel dependencies"""
    start_time = time.time()

    make_bid_max_prices()

    for unit in Gu:
        # try:
            # for gu in gu_lst:
        unit.set_parent_dgu(Dgu)
        # except:
        #     print(gu)

    for line in Line:
        line.set_nodes(Node)

    for dgu in Dgu:
        dgu.set_node(Node)
        dgu.set_wsumgen(Wsumgen)
        dgu.set_parent_dpg(DpgSupply)

    for dpg in list(Dpg):  # .lst['id'].values():
        dpg.set_bid(Bid)

    for dpg in DpgDemand:
        dpg.set_consumer(Consumer)
        dpg.set_load(Load)
        dpg.set_area(Area)

    for area in Area:
        area.attach_nodes(Node)
        area.set_impex_data(ImpexArea)

    for load in Load:
        load.set_nodes(Node)
        load.recalculate()

    for dpg in DpgDemandLoad:
        dpg.recalculate()
    for dpg in DpgDemandSystem:
        dpg.recalculate()

    for dpg in DpgSupply:
        dpg.set_station(Station)
        dpg.set_fed_station(DpgDemand)
        dpg.set_dpg_demand(DpgDemand)

    for dgu in Dgu:
        dgu.modify_state()

    for dpg in DpgSupply:
        dpg.recalculate()

    for dpg in DpgImpex:
        dpg.set_section(Section)

    for section in Section:
        # section.set_max_price(Bid.max_prices)
        section.attach_lines(Line)
        section.set_impex_data(ImpexArea)

    for wsumgen in Wsumgen:
        wsumgen.recalculate()

    # for node in Node:
    #     node.modify_state()

    print(time.time() - start_time)

def fill_db(tdate):
    """insert data into DB"""
    start_time = time.time()

    con = DB.OracleConnection()

    send_bids_to_db(con, tdate)
    send_dpgs_to_db(con, tdate)
    send_stations_to_db(con, tdate)
    send_consumers_to_db(con)
    send_loads_to_db(con)
    send_areas_to_db(con)
    send_nodes_to_db(con)
    send_lines_to_db(con)
    send_dgus_to_db(con, tdate)
    send_dpgs_to_kc_dpg_node(con)
    send_dpgs_to_kg_dpg_rge(con)
    send_dpgs_to_node_bid_pair(con)

    if tdate.year < 2017:
        with con.cursor() as curs:
            curs.execute('''
                UPDATE trader
                set oes = 3
                where region_code in (35, 67)
            ''')
            curs.execute('''
                DELETE from region
                where region_code in (35, 67)
            ''')
            curs.executemany('''
                INSERT into region (region_code, region_name, sort_order)
                values (:1, :2, :3)
            ''', [(35, 'Республика Крым', 31), (67, 'Город Севастополь', 56)])

    con.commit()
    print(time.time() - start_time)

# def modify_block_states():
#     for dgu in Dgu:
#         # dgu.deplete()
#         dgu.modify_state()
