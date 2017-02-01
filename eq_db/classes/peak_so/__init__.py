"""Create PeakSO instances."""
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import peak_so_script as pss
from .peak_so import PeakSO


@ts_manager
def make_peak_so(tsid, tdate):
    """create Settings instances"""
    con = DB.OracleConnection()
    PeakSO.clear()

    for new_row in con.script_cursor(pss, tsid=tsid, tdate=tdate):
        PeakSO(new_row)
