"""Create Area instances."""
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import rastr_areas_script as ra
from .areas import Area


@ts_manager
def make_areas(tsid):
    """create Area instances"""
    con = DB.OracleConnection()
    Area.clear()

    for new_row in con.script_cursor(ra, tsid=tsid):
        area = Area[new_row.area]
        if not area:
            area = Area(new_row)
        area.add_area_hour_data(new_row)
