"""Create Dpg instances."""
from operator import attrgetter
import cx_Oracle
import constants as C
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import consumers_script as cs
from sql_scripts import consumers_script_v as cs_v
from sql_scripts import disqualified_data_script as dds
from sql_scripts import generators_script as gs
from sql_scripts import generators_script_v as gs_v
from sql_scripts import impex_dpgs_script as imp_s

from .base_dpg import Dpg
from .dpg_demand import DpgDemand
from .dpg_demand_fsk import DpgDemandFSK
from .dpg_demand_system import DpgDemandSystem
from .dpg_demand_load import DpgDemandLoad
from .dpg_supply import DpgSupply
from .dpg_impex import DpgImpex


PARTICIPANTS_TO_ADD = [(C.SIPR_OWNER_ID, C.SIPR_OWNER_CODE), (464690, 'KRYMITEC'),
                       (464693, 'SEVENSBT'), (464691, 'KRYMENRG'), (464692, 'SGSPLYUS')]

@ts_manager
def make_dpgs(tsid):
    """create Dpg instances"""
    con = DB.OracleConnection()

    for new_row in con.script_cursor(cs, tsid=tsid):
        if new_row.is_fsk:
            DpgDemandFSK(new_row)
        elif new_row.is_system:
            DpgDemandSystem(new_row)
        else:
            DpgDemandLoad(new_row)

    for new_row in con.script_cursor(dds, tsid=tsid):
        Dpg.by_id[new_row.dpg_id].add_disqualified_data(new_row)

    for new_row in con.script_cursor(gs, tsid=tsid):
        DpgSupply(new_row)

    for new_row in con.script_cursor(imp_s, tsid=tsid):
        DpgImpex(new_row)

@ts_manager
def add_dpgs_vertica(scenario):
    """add Dpg instances from Vertica DB"""
    con = DB.VerticaConnection()

    for new_row in con.script_cursor(gs_v, scenario=scenario):
        DpgSupply(new_row, is_new=True)

    for new_row in con.script_cursor(cs_v, scenario=scenario):
        if new_row.is_fsk:
            DpgDemandFSK(new_row, is_new=True)
        elif new_row.is_system:
            DpgDemandSystem(new_row, is_new=True)
        else:
            DpgDemandLoad(new_row, is_new=True)

    if  Dpg.by_code['PKRYMEN1'] and Dpg.by_code['PAPBESK1']:
        Dpg.by_code['PAPBESK1'].remove()

@ts_manager
def send_dpgs_to_db(ora_con, tdate):
    """save new instances to current session"""

    supplies_data = []
    const_part = (tdate, tdate, C.DPG_TRADER_TYPE, C.DPG_SUPPLY_TYPE, 0)
    attrs = '''_id _id code price_zone_code is_gaes is_blocked
            is_unpriced_zone fed_station_id station_id is_spot_trader
            dpg_demand_id participant_id region_code oes_code name'''.split()
    atg = attrgetter(*attrs)
    supplies_attrs_len = len(const_part) + len(attrs)

    for dpg in DpgSupply:
        if dpg.is_new:
            supplies_data.append(
                const_part + atg(dpg)
            )

    demands_data = []
    const_part = (tdate, tdate, C.DPG_TRADER_TYPE, C.DPG_DEMAND_TYPE, 0)
    attrs = '''_id _id code price_zone_code consumer_code
            area_code dem_rep_volume dem_rep_hours is_system
            is_gp is_fed_station is_disqualified is_unpriced_zone
            is_fsk is_spot_trader region_code participant_id area_external'''.split()
    atg = attrgetter(*attrs)
    demands_attrs_len = len(const_part) + len(attrs) + 1

    for dpg in DpgDemand:
        if dpg.is_new:
            is_gaes = 1 if dpg.supply_gaes else None
            demands_data.append(
                const_part + atg(dpg) + (is_gaes,)
            )

    with ora_con.cursor() as curs:
        curs.execute('''
            DELETE from trader
            where start_version is null
            and trader_type in (:type1, :type2)
        ''', type1=C.PARTICIPANT_TYPE, type2=C.DPG_TRADER_TYPE)

        if not Dpg.by_code['PAPBESK1']:
            curs.execute('''
                DELETE from trader
                where trader_code = 'PAPBESK1'
            ''')

        curs.executemany('''
            INSERT into trader (trader_id, real_trader_id,
            begin_date, end_date, trader_type, trader_code)
            values (:1, :2, :3, :4, :5, :6)
        ''', [(prt_id, prt_id, tdate, tdate, C.PARTICIPANT_TYPE, prt_code) \
              for prt_id, prt_code in PARTICIPANTS_TO_ADD])

        curs.executemany('''
            INSERT into trader (begin_date, end_date, trader_type, dpg_type, is_impex,
            trader_id, real_trader_id, trader_code, price_zone_code, is_gaes,
            is_blocked, is_unpriced_zone, ownneeds_dpg_id, dpg_station_id,
            is_spot_trader, parent_dpg_id, parent_object_id, region_code,
            oes, full_name)
            values (:{})
        '''.format(', :'.join(str(i + 1) for i in range(supplies_attrs_len))),
                         supplies_data)

        curs.executemany('''
            INSERT into trader (begin_date, end_date, trader_type, dpg_type, is_impex,
            trader_id, real_trader_id, trader_code, price_zone_code, consumer2,
            es_ref, dpg_dr_volume_decr_fact, dpg_dr_hours_decr, is_system,
            is_guarantee_supply_co, fed_station, is_disqualified, is_unpriced_zone,
            is_fsk, is_spot_trader, region_code, parent_object_id, es_ref_ex, is_gaes)
            values (:{})
        '''.format(', :'.join(str(i + 1) for i in range(demands_attrs_len))),
                         demands_data)

@ts_manager
def send_dpgs_to_kc_dpg_node(ora_con):
    """fill kc_dpg_node"""
    script = """
        INSERT into kc_dpg_node (hour, kc, kc_nodedose, node, sta,
            dpg_id, is_system, dpg_code, consumer2)
        VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)"""

    attrs = 'hour k_distr k_distr'
    atg = attrgetter(*attrs.split())
    dpg_attrs = '_id is_system code consumer_code'
    dpg_atg = attrgetter(*dpg_attrs.split())

    data_load = []
    for dpg in DpgDemandLoad:
        if dpg.is_unpriced_zone:
            continue
        dpg_part = dpg_atg(dpg)
        if dpg.supply_gaes:
            for dgu in dpg.supply_gaes.dgus:
                for _hd in dgu.hour_data:
                    k_distr = _hd.kg if _hd.kg else dgu.kg_fixed
                    node_state = not dgu.node.hour_data[_hd.hour].state
                    data_load.append((
                        _hd.hour, k_distr, k_distr, dgu.node.code, node_state
                    ) + dpg_part)
        else:
            for _nd in dpg.load.nodes_data:
                for _hd in _nd.hour_data:
                    node_part = (_nd.node_code, not _nd.node.hour_data[_hd.hour].state)
                    data_load.append(atg(_hd) + node_part + dpg_part)

    data_sys = []
    for dpg in DpgDemandSystem:
        if dpg.is_unpriced_zone and dpg.code not in C.UNPRICED_AREA:
            continue
        dpg_part = dpg_atg(dpg)
        for node in dpg.area.nodes:
            for _hd in node.hour_data:
                node_part = (node.code, not _hd.state)
                data_sys.append(atg(_hd) + node_part + dpg_part)

    with ora_con.cursor() as curs:
        curs.execute('DELETE from kc_dpg_node')

        print('insert DpgDemandLoad')
        curs.executemany(script, data_load)

        print('insert DpgDemandSystem')
        curs.executemany(script, data_sys)

@ts_manager
def send_dpgs_to_kg_dpg_rge(ora_con):
    """fill kg_dpg_rge"""
    script = """
        INSERT into kg_dpg_rge (hour, kg, p, pmax, pmin, pminagg, dpminso,
                    kg_min, kg_reg, dpmin_heat, dpmin_tech, dpg_id, node,
                    rge_id, rge_code, sta, kg_fixed_power)
        VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14,
                :15, :16, :17)"""

    dgu_attrs = 'dpg_id node.code _id code'
    dgu_atg = attrgetter(*dgu_attrs.split())

    attrs = '''hour kg
             p pmax pmin pmin_agg pmin_so kg_min kg_reg
             pmin_heat pmin_tech'''
    atg = attrgetter(*attrs.split())

    data = []
    for dpg in DpgSupply:
        for dgu in dpg.dgus:
            if not dgu.hour_data:
                continue
            common_data = dgu_atg(dgu)
            for _hd in dgu.hour_data:
                node_state = dgu.node.hour_data[_hd.hour].state
                data.append(atg(_hd) + common_data +
                            ((0, dgu.kg_fixed) if node_state else (1, None)))

    with ora_con.cursor() as curs:
        curs.execute('DELETE from kg_dpg_rge')
        curs.executemany(script, data)

@ts_manager
def send_dpgs_to_node_bid_pair(ora_con):
    """fill node_bid_pair"""
    supply_distr_script = """
        INSERT into node_bid_pair (hour, node, volume, min_volume,
            price, num, interval_num, dpg_code, price_zone_mask)
        values (:1, :2, :3, :4, :5, :6, :7, :8, :9)"""

    supply_distr = []
    for dpg in DpgSupply:
        for bid in dpg.get_distributed_bid():
            if not bid[7]: # not integral bid
                supply_distr.append(bid[:7] + (dpg.code, dpg.price_zone_code * 2))


    demand_distr_script = '''
        INSERT into node_bid_pair (hour, consumer2, interval_num,
            node, volume, price, dpg_code, price_zone_mask)
        values (:1, :2, :3, :4, :5, :6, :7, :8)'''

    demand_distr = []
    for dpg in DpgDemand:
        for bid in dpg.get_distributed_bid():
            if dpg.supply_gaes:
                (dgu,) = dpg.supply_gaes.dgus
                supply_distr.append((
                    bid[0], bid[4], -bid[5], -bid[5], 0, dgu.code,
                    bid[3], dpg.supply_gaes.code, dpg.price_zone_code * 2
                ))
            else:
                demand_distr.append(bid[:1] + bid[2:-1] + (dpg.code, dpg.price_zone_code * 2))

    for dpg in DpgDemandFSK:
        if not dpg.area:
            continue
        for node in dpg.area.nodes:
            for _hd in node.hour_data:
                demand_distr.append((
                    _hd.hour, None, None, node.code, _hd.pn, 0, dpg.code, node.price_zone * 2
                ))

    with ora_con.cursor() as curs:
        curs.execute('DELETE from node_bid_pair')

        print('insert DpgSupply')
        curs.executemany(supply_distr_script, supply_distr)

        print('insert DpgDemand')
        curs.executemany(demand_distr_script, demand_distr)

        print('correct node_bid_pair volumes')
        curs.execute('''
            MERGE INTO node_bid_pair n1
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
