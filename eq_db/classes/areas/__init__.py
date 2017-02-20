"""Create Area instances."""
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import rastr_areas_script as ra
from sql_scripts import rastr_areas_script_v as ra_v
from .areas import Area


@ts_manager
def make_areas(tsid):
    """create Area instances"""
    con = DB.OracleConnection()

    for new_row in con.script_cursor(ra, tsid=tsid):
        area = Area[new_row.area]
        if not area:
            area = Area(new_row)
        area.add_area_hour_data(new_row)

@ts_manager
def add_areas_vertica(scenario):
    """add Area instances from Vertica DB"""
    con = DB.VerticaConnection()
    for new_row in con.script_cursor(ra_v, scenario=scenario):
        area = Area[new_row.area]
        if not area:
            area = Area(new_row, is_new=True)
        area.add_area_hour_data(new_row)
