"""Create Gu instances."""
from utils import DB
from utils.printer import print
from utils.trade_session_manager import ts_manager
from sql_scripts import gus_script as gs
from sql_scripts import nblock_script as ns
from .gus import Gu
from .gus_hour_data import GuHourData


@ts_manager
def make_gus(tsid):
    """create Gu instances"""
    con = DB.OracleConnection()
    Gu.clear()

    for new_row in con.script_cursor(gs, tsid=tsid):
        Gu(new_row)

    for new_row in con.script_cursor(ns, tsid=tsid):
        gu_code = new_row.gu_code
        if not Gu['code', gu_code]:
            Gu.from_nblock(new_row)
            print('WARNING!! added Gu %i not from RIO!' % gu_code)
        unit_hd = GuHourData(new_row)
        for unit in Gu['code', gu_code]:
            unit.add_gu_hour_data(new_row, unit_hd)
