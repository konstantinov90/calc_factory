"""Initialize and intertwine model."""
import time
from concurrent import futures

from utils.progress_bar import update_progress
from eq_db.classes.sections import make_sections, Section
from eq_db.classes.dgu_groups import make_dgu_groups, DguGroup
from eq_db.classes.bids import make_bids, add_bids_vertica, Bid
from eq_db.classes.stations import make_stations, add_stations_vertica, Station
from eq_db.classes.areas import make_areas, Area
from eq_db.classes.impex_areas import make_impex_areas, ImpexArea
from eq_db.classes.nodes import make_nodes, add_nodes_vertica, Node
from eq_db.classes.loads import make_loads, Load
from eq_db.classes.consumers import make_consumers, Consumer
from eq_db.classes.dpgs import make_dpgs, add_supplies_vertica
from eq_db.classes.dpgs.base_dpg import Dpg
from eq_db.classes.dpgs.dpg_demand import DpgDemand
from eq_db.classes.dpgs.dpg_demand_fsk import DpgDemandFSK
from eq_db.classes.dpgs.dpg_demand_system import DpgDemandSystem
from eq_db.classes.dpgs.dpg_demand_load import DpgDemandLoad
from eq_db.classes.dpgs.dpg_supply import DpgSupply
from eq_db.classes.dpgs.dpg_impex import DpgImpex
from eq_db.classes.dgus import make_dgus, add_dgus_vertica, Dgu
from eq_db.classes.wsumgen import make_wsumgen, Wsumgen
from eq_db.classes.gus import make_gus, Gu
from eq_db.classes.lines import make_lines, add_lines_vertica, Line
from eq_db.classes.price_zone import make_price_zones, PriceZone
from eq_db.classes.settings import make_settings, Setting
from eq_db.classes.bids_max_prices import make_bid_max_prices, BidMaxPrice
from utils import DB

# tsid = 221076901
# scenario = 1
# tdate = '29-05-2015'
def initialize_model(tsid, scenario, target_date, use_vertica):
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


    ora_con = DB.OracleConnection()

    try:

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
        make_price_zones(tdate=tdate)
        make_settings(tsid, tdate=tdate)
        if use_vertica:
            add_bids_vertica(scenario, target_date, tdate=tdate)
            add_stations_vertica(scenario, tdate=tdate, target_date=target_date, ora_con=ora_con)
            add_nodes_vertica(scenario, tdate=tdate)
            add_dgus_vertica(scenario, tdate=tdate, target_date=target_date, ora_con=ora_con)
            add_supplies_vertica(scenario, tdate=tdate, target_date=target_date, ora_con=ora_con)
            add_lines_vertica(scenario, tdate=tdate)
        make_bid_max_prices(tdate=tdate)
    except Exception:
        ora_con.rollback()
        raise
    else:
        ora_con.commit()

    print(time.time() - start_time)

def intertwine_model():
    """set intramodel dependencies"""
    start_time = time.time()

    # Bid.set_max_price()

    for unit in Gu:
        # try:
            # for gu in gu_lst:
        unit.set_parent_dgu(Dgu)
        # except:
        #     print(gu)

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

    for line in Line:
        line.set_nodes(Node)

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

    for dpg in DpgImpex:
        dpg.set_section(Section)

    for section in Section:
        # section.set_max_price(Bid.max_prices)
        section.attach_lines(Line)
        section.set_impex_data(ImpexArea)

    print(time.time() - start_time)
