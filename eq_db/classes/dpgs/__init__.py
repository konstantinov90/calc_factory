"""Create Dpg instances."""
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import consumers_script as cs
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
SIPR_OWNER_ID = 987654321
PARTICIPANT_TYPE = 2
SIPR_OWNER_CODE = 'SIPROWNE'


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
def add_supplies_vertica(scenario, **kwargs):
    """add DpgSupplies instances from Vertica DB"""
    con = DB.VerticaConnection()
    ora_con = kwargs['ora_con']
    tdate = kwargs['target_date']

    ora_con.exec_insert('''insert into trader (trader_id, real_trader_id,
                        begin_date, end_date, trader_type, trader_code)
                        values (:id, :id, :tdate, :tdate, :type, :code)
                        ''', id=SIPR_OWNER_ID, tdate=tdate, type=PARTICIPANT_TYPE,
                        code=SIPR_OWNER_CODE)

    for new_row in con.script_cursor(gs_v, scenario=scenario):
        if not ora_con.exec_script('''
                select trader_id from trader where trader_code=:trader_code
                ''', trader_code=new_row.dpg_code):
            ora_con.exec_insert('''
                insert into trader (trader_id, real_trader_id, trader_code,
                begin_date, end_date, trader_type, price_zone_code, is_gaes,
                is_blocked, is_unpriced_zone, ownneeds_dpg_id, dpg_station_id,
                is_spot_trader, parent_dpg_id, dpg_type, parent_object_id, region_code)
                values (:gtp_id, :gtp_id, :dpg_code, :tdate, :tdate, :trader_type,
                    :price_zone_code, :is_gaes, :is_blocked, :is_unpriced_zone,
                    :fed_station_id, :station_id, :is_spot_trader, :dpg_demand_id,
                    :dpg_type, :parent_id, :region_code)
                ''', tdate=tdate, trader_type=DPG_TRADER_TYPE, parent_id=SIPR_OWNER_ID,
                                dpg_type=DPG_SUPPLY_TYPE, **new_row._asdict())

        DpgSupply(new_row)
