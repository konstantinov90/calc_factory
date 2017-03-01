"""Create Dpg instances."""
from operator import attrgetter
import cx_Oracle
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

DPG_TRADER_TYPE = 100
DPG_SUPPLY_TYPE = 2
DPG_DEMAND_TYPE = 1
SIPR_OWNER_ID = 987654321
PARTICIPANT_TYPE = 2
SIPR_OWNER_CODE = 'SIPROWNE'
NOT_IMPEX = 0

PARTICIPANTS_TO_ADD = [(SIPR_OWNER_ID, SIPR_OWNER_CODE), (464690, 'KRYMITEC'),
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

@ts_manager
def send_dpgs_to_db(ora_con, tdate):
    """save new instances to current session"""

    supplies_data = []
    const_part = (tdate, tdate, DPG_TRADER_TYPE, DPG_SUPPLY_TYPE, NOT_IMPEX)
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
    const_part = (tdate, tdate, DPG_TRADER_TYPE, DPG_DEMAND_TYPE, NOT_IMPEX)
    attrs = '''_id _id code price_zone_code consumer_code
            area_code dem_rep_volume dem_rep_hours is_system
            is_gp is_fed_station is_disqualified is_unpriced_zone
            is_fsk is_spot_trader region_code participant_id area_external'''.split()
    atg = attrgetter(*attrs)
    demands_attrs_len = len(const_part) + len(attrs)

    remove_papbesk = False
    for dpg in DpgDemand:
        if dpg.is_new:
            demands_data.append(
                const_part + atg(dpg)
            )
            if dpg.code == 'PKRYMEN1' and Dpg.by_code['PAPBESK1']:
                remove_papbesk = True
    if remove_papbesk:
        Dpg.by_code['PAPBESK1'].remove()
        ora_con.exec_insert('''
            DELETE from trader
            where trader_code = 'PAPBESK1'
        ''')


    with ora_con.cursor() as curs:
        curs.execute('''
            DELETE from trader
            where start_version is null
            and trader_type in (:type1, :type2)
        ''', type1=PARTICIPANT_TYPE, type2=DPG_TRADER_TYPE)



        curs.executemany('''
            INSERT into trader (trader_id, real_trader_id,
            begin_date, end_date, trader_type, trader_code)
            values (:1, :2, :3, :4, :5, :6)
        ''', [(prt_id, prt_id, tdate, tdate, PARTICIPANT_TYPE, prt_code) \
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
            is_fsk, is_spot_trader, region_code, parent_object_id, es_ref_ex)
            values (:{})
        '''.format(', :'.join(str(i + 1) for i in range(demands_attrs_len))),
                         demands_data)
