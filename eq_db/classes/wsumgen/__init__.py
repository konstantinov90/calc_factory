"""Create Wsumgen instances."""
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import wsumgen_script as ws
from .wsumgen import Wsumgen


@ts_manager
def make_wsumgen(tsid):
    """create Wsumgen instances"""
    con = DB.OracleConnection()

    for new_row in con.script_cursor(ws, tsid=tsid):
        Wsumgen(new_row)
