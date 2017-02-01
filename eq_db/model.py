"""Initialize and intertwine model."""
import time
from concurrent import futures
from itertools import chain

from utils import DB
from utils.progress_bar import update_progress
from eq_db.classes.sections import make_sections, add_sections_vertica, Section
from eq_db.classes.dgu_groups import make_dgu_groups, DguGroup
from eq_db.classes.bids import make_bids, add_bids_vertica, Bid
from eq_db.classes.stations import make_stations, add_stations_vertica, Station
from eq_db.classes.areas import make_areas, add_areas_vertica, Area
from eq_db.classes.impex_areas import make_impex_areas, ImpexArea
from eq_db.classes.nodes import make_nodes, add_nodes_vertica, Node
from eq_db.classes.loads import make_loads, Load
from eq_db.classes.consumers import make_consumers, add_consumers_vertica, Consumer
from eq_db.classes.dpgs import make_dpgs, add_dpgs_vertica
from eq_db.classes.dpgs.base_dpg import Dpg
from eq_db.classes.dpgs.dpg_demand import DpgDemand
from eq_db.classes.dpgs.dpg_demand_fsk import DpgDemandFSK
from eq_db.classes.dpgs.dpg_demand_system import DpgDemandSystem
from eq_db.classes.dpgs.dpg_demand_load import DpgDemandLoad
from eq_db.classes.dpgs.dpg_supply import DpgSupply
from eq_db.classes.dpgs.dpg_impex import DpgImpex
from eq_db.classes.dgus import make_dgus, add_dgus_vertica, Dgu
from eq_db.classes.wsumgen import make_wsumgen, Wsumgen
from eq_db.classes.gus import make_gus, add_gus_vertica, Gu
from eq_db.classes.lines import make_lines, add_lines_vertica, Line
from eq_db.classes.price_zone import make_price_zones, PriceZone
from eq_db.classes.settings import make_settings, Setting
from eq_db.classes.bids_max_prices import make_bid_max_prices, BidMaxPrice
from eq_db.classes.peak_so import make_peak_so, PeakSO

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

    if use_vertica:
        ora_con = DB.OracleConnection()
        with ora_con.cursor() as curs:
            curs.execute("DELETE from trader where full_name is null")
            curs.execute("DELETE from rastr_vetv where loading_protocol is null")
            curs.execute("DELETE from rastr_node where loading_protocol is null")
            curs.execute("DELETE from rastr_consumer2 where loading_protocol is null")
        ora_con.commit()
    # read only!
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

    if use_vertica: # read-write operations!
        try:
            add_areas_vertica(scenario, tdate=tdate, target_date=target_date, ora_con=ora_con)
            add_dpgs_vertica(scenario, tdate=tdate, target_date=target_date, ora_con=ora_con)
            add_consumers_vertica(scenario, tdate=tdate, target_date=target_date, ora_con=ora_con)
            add_sections_vertica(scenario, tdate=tdate, target_date=target_date, ora_con=ora_con)
            add_bids_vertica(scenario, tdate=tdate, target_date=target_date, ora_con=ora_con)
            add_stations_vertica(scenario, tdate=tdate, target_date=target_date, ora_con=ora_con)
            add_nodes_vertica(scenario, tdate=tdate, target_date=target_date, ora_con=ora_con)
            add_dgus_vertica(scenario, tdate=tdate, target_date=target_date, ora_con=ora_con)
            add_lines_vertica(scenario, tdate=tdate, target_date=target_date, ora_con=ora_con)
            add_gus_vertica(scenario)

        except Exception:
            ora_con.rollback()
            raise
        else:
            ora_con.commit()

    make_bid_max_prices(tdate=tdate)

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

    for line in Line:
        line.set_nodes(Node)

    for dgu in Dgu:
        dgu.set_node(Node)
        dgu.set_wsumgen(Wsumgen)
        dgu.set_parent_dpg(DpgSupply)

    for dgu in Dgu:
        dgu.modify_state()

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

def fill_db():
    """insert data into DB"""
    con = DB.OracleConnection()

    con.exec_insert('DELETE from kc_dpg_node')
    for i, dpg in enumerate(chain(DpgDemandLoad, DpgDemandSystem)):
        dpg.fill_db(con)
        update_progress((i + 1) / (len(DpgDemandLoad) + len(DpgDemandSystem)))

    con.exec_insert('DELETE from kg_dpg_rge')
    for i, dgu in enumerate(Dgu):
        dgu.fill_db(con)
        update_progress((i + 1) / len(Dgu))


    con.exec_insert('DELETE from node_bid_pair')
    _es = []
    _ed = []
    for dpg in DpgSupply:
        for bid in dpg.get_distributed_bid():
            if not bid[7]:
                _es.append(bid[:7] + (dpg.code, Node[bid[1]].price_zone * 2))

    for dpg in DpgDemand:
        for bid in dpg.get_distributed_bid():
            if dpg.supply_gaes:
                _es.append((
                    bid[0], bid[4], -bid[5], -bid[5], 0, dpg.supply_gaes.dgus[0].code,
                    bid[3], dpg.supply_gaes.code, Node[bid[4]].price_zone * 2
                ))
            else:
                _ed.append(bid[:1] + bid[2:-1] + (dpg.code, Node[bid[4]].price_zone * 2))

    for dpg in DpgDemandFSK:
        if not dpg.area:
            continue
        for node in dpg.area.nodes:
            for _hd in node.hour_data:
                _ed.append((
                    _hd.hour, None, None, node.code, _hd.pn, 0, dpg.code, node.price_zone * 2
                ))

    with con.cursor() as curs:
        curs.executemany('''INSERT into node_bid_pair (hour, node, volume, min_volume,
                            price, num, interval_num, dpg_code, price_zone_mask)
                            values (:1, :2, :3, :4, :5, :6, :7, :8, :9)
                         ''', _es)
        curs.executemany('''INSERT into node_bid_pair (hour, consumer2, interval_num,
                            node, volume, price, dpg_code, price_zone_mask)
                            values (:1, :2, :3, :4, :5, :6, :7, :8)
                         ''', _ed)
        curs.execute('''MERGE INTO node_bid_pair n1
                    using (SELECT sum(volume) over (partition by dpg_code, num, node, hour order by interval_num) v,
                                dpg_Code, num, consumer2, node, interval_num, hour
                          from node_bid_pair) n2
                    on (n1.dpg_code = n2.dpg_code
                    and nvl(n1.num,0) = nvl(n2.num,0)
                    and nvl(n1.node,0) = nvl(n2.node,0)
                    and nvl(n1.consumer2,0) = nvl(n2.consumer2,0)
                    and nvl(n1.interval_num,999) = nvl(n2.interval_num,999)
                    and n1.hour = n2.hour)
                    when matched then update
                    set n1.volume = n2.v''')


    con.commit()

# def modify_block_states():
#     for dgu in Dgu:
#         # dgu.deplete()
#         dgu.modify_state()
