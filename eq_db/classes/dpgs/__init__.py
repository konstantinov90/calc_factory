"""Create Dpg instances."""
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

PARTICIPANTS_TO_ADD = [(SIPR_OWNER_ID, SIPR_OWNER_CODE), (464690, 'KRYMITEC'),
                       (464693, 'SEVENSBT'), (464691, 'KRYMENRG'), (464692, 'SGSPLYUS')]


@ts_manager
def make_dpgs(tsid):
    """create Dpg instances"""
    con = DB.OracleConnection()
    Dpg.clear()
    DpgSupply.clear()
    DpgImpex.clear()
    DpgDemand.clear()
    DpgDemandFSK.clear()
    DpgDemandSystem.clear()
    DpgDemandLoad.clear()

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
def add_dpgs_vertica(scenario, **kwargs):
    """add Dpg instances from Vertica DB"""
    con = DB.VerticaConnection()
    ora_con = kwargs['ora_con']
    tdate = kwargs['target_date']

    for prt_id, prt_code in PARTICIPANTS_TO_ADD:
        ora_con.exec_insert('''insert into trader (trader_id, real_trader_id,
                            begin_date, end_date, trader_type, trader_code)
                            values (:id, :id, :tdate, :tdate, :type, :code)
                            ''', id=prt_id, tdate=tdate, type=PARTICIPANT_TYPE,
                            code=prt_code)

    for new_row in con.script_cursor(gs_v, scenario=scenario):
        if not ora_con.exec_script('''
                select trader_id from trader where trader_code=:trader_code
                ''', trader_code=new_row.dpg_code):
            try:
                ora_con.exec_insert('''
                    insert into trader (trader_id, real_trader_id, trader_code,
                    begin_date, end_date, trader_type, price_zone_code, is_gaes,
                    is_blocked, is_unpriced_zone, ownneeds_dpg_id, dpg_station_id,
                    is_spot_trader, parent_dpg_id, dpg_type, parent_object_id, region_code)
                    values (:gtp_id, :gtp_id, :dpg_code, :tdate, :tdate, :trader_type,
                        :price_zone_code, :is_gaes, :is_blocked, :is_unpriced_zone,
                        :fed_station_id, :station_id, :is_spot_trader, :dpg_demand_id,
                        :dpg_type, :participant_id, :region_code)
                    ''', tdate=tdate, trader_type=DPG_TRADER_TYPE, # parent_id=SIPR_OWNER_ID,
                                    dpg_type=DPG_SUPPLY_TYPE, **new_row._asdict())
            except cx_Oracle.IntegrityError:
                print(new_row)
                raise

        DpgSupply(new_row)

    for new_row in con.script_cursor(cs_v, scenario=scenario):
        if not ora_con.exec_script('''
                select trader_id from trader where trader_code = :trader_code
                ''', trader_code=new_row.dpg_code):
            ora_con.exec_insert('''
                insert into trader (trader_id, real_trader_id, trader_code,
                begin_date, end_date, trader_type, price_zone_code, consumer2,
                es_ref, dpg_dr_volume_decr_fact, dpg_dr_hours_decr, is_system,
                is_guarantee_supply_co, fed_station, is_disqualified, is_unpriced_zone,
                is_fsk, is_spot_trader, region_code, dpg_type, parent_object_id,
                TARIFF2_SUPPLY_MGI, HELP_IMPORT_PRICE2, is_impex)
                values (:dpg_id, :dpg_id, :dpg_code, :tdate, :tdate, :trader_type,
                    :price_zone_code, :consumer_code, :area, :dem_rep_volume,
                    :dem_rep_hours, :is_system, :is_gp, :is_fed_station, :is_disqualified,
                    :is_unpriced_zone, :is_fsk, :is_spot_trader, :region_code, :dpg_type,
                    :participant_id, :min_forecast, :max_forecast, :is_impex)
            ''', tdate=tdate, trader_type=DPG_TRADER_TYPE, dpg_type=DPG_DEMAND_TYPE,
                                is_impex=0, # parent_id=SIPR_OWNER_ID,
                                **new_row._asdict())
        if new_row.dpg_code == 'PKRYMEN1' and Dpg.by_code['PAPBESK1']:
            Dpg.by_code['PAPBESK1'].remove()
            ora_con.exec_insert("delete from trader where trader_code = 'PAPBESK1'")

        if new_row.is_fsk:
            DpgDemandFSK(new_row)
        elif new_row.is_system:
            DpgDemandSystem(new_row)
        else:
            DpgDemandLoad(new_row)
