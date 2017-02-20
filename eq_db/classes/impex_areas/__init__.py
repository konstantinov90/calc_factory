"""Create ImpexArea instances."""
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import impex_area_script as ias
from .impex_areas import ImpexArea


@ts_manager
def make_impex_areas(tsid):
    """create ImpexArea instances"""
    con = DB.OracleConnection()

    for new_row in con.script_cursor(ias, tsid=tsid):
        ImpexArea(new_row)
