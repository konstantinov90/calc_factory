"""Create Load instances."""
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import rastr_load_script as rl
from .loads import Load


@ts_manager
def make_loads(tsid):
    """create Load instances"""
    con = DB.OracleConnection()
    Load.clear()

    for new_row in con.script_cursor(rl, tsid=tsid):
        load = Load[new_row.consumer_code]
        if not load:
            load = Load(new_row)
        load.add_load_hour_data(new_row)
