"""Create PeakSO instances."""
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import peak_so_script as pss, peak_so_backup_script as pbs
from .peak_so import PeakSO


@ts_manager
def make_peak_so(tsid, tdate):
    """create Settings instances"""
    con = DB.OracleConnection()
    PeakSO.clear()
    done = False

    for new_row in con.script_cursor(pss, tsid=DB.Partition(tsid), tdate=tdate):
        PeakSO(new_row)
        done = True

    if not done:
        for new_row in con.script_cursor(pbs):
            PeakSO(new_row)
