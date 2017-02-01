"""Create Dgu instances."""
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import dgus_script as dgs
from sql_scripts import dgus_script_v as dgs_v
from sql_scripts import rastr_gen_script as rgs
from sql_scripts import rastr_gen_script_v as rgs_v
from sql_scripts import generators_last_hour_script as glhs
from sql_scripts import hydro_new_volume_script as hnvs
from .dgus import Dgu

DGU_TRADER_TYPE = 103


@ts_manager
def make_dgus(tsid):
    """create Dgu instances"""
    con = DB.OracleConnection()
    Dgu.clear()

    for new_row in con.script_cursor(dgs, tsid=tsid):
        is_new_dgu = False
        Dgu(new_row, is_new_dgu)

    for new_row in con.script_cursor(rgs, tsid=tsid):
        dgu = Dgu.by_code[new_row.rge_code]
        if dgu:
            dgu.add_dgu_hour_data(new_row)

    for new_row in con.script_cursor(glhs, tsid=tsid):
        dgu = Dgu.by_code[new_row.rge_code]
        if dgu:
            dgu.set_last_hour(new_row)

@ts_manager
def add_dgus_vertica(scenario, **kwargs):
    """add Dgu instances from Vertica DB"""
    con = DB.VerticaConnection()
    ora_con = kwargs['ora_con']
    tdate = kwargs['target_date']


    for new_row in con.script_cursor(dgs_v, scenario=scenario):
        is_new_dgu = True
        if not ora_con.exec_script('''
                    select trader_id from trader where trader_type = 103 and trader_code=:trader_code
                ''', trader_code=new_row.code):
            ora_con.exec_insert('''
                insert into trader(trader_id, real_trader_id, trader_code, parent_object_id,
                fixed_power, begin_date, end_date, trader_type)
                values(:id, :id, :code, :dpg_id, :fixed_power, :tdate, :tdate, :trader_type)
                ''', tdate=tdate,
                    trader_type=DGU_TRADER_TYPE, **new_row._asdict())


        print(Dgu(new_row, is_new_dgu))

    for new_row in con.script_cursor(rgs_v, scenario=scenario):
        dgu = Dgu.by_code[new_row.rge_code]
        if dgu:
            dgu.add_dgu_hour_data(new_row)

    for new_row in con.script_cursor(hnvs, scenario=scenario):
        dgu = Dgu.by_code[new_row.dgu_code]
        if not dgu:
            raise Exception('vertica changes volume for nonexistent dgu %i' % new_row.dgu_code)
        dgu_hd = dgu.hour_data[new_row.hour]
        dgu_hd.p = new_row.volume
        dgu_hd.pmax = max(new_row.volume, dgu_hd.pmax)
