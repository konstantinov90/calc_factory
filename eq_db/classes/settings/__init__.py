"""Create Settings instances."""
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import settings_script as ss
from .settings import Setting


@ts_manager
def make_settings(tsid):
    """create Settings instances"""
    con = DB.OracleConnection()
    Setting.clear()

    for new_row in con.script_cursor(ss, tsid=tsid):
        Setting(new_row)
