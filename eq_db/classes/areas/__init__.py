"""Create Area instances."""
from operator import attrgetter

import constants
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import rastr_areas_script as ra
from sql_scripts import rastr_areas_script_v as ra_v
from .areas import Area


@ts_manager
def make_areas(tsid):
    """create Area instances"""
    con = DB.OracleConnection()
    Area.clear()

    for new_row in con.script_cursor(ra, tsid=DB.Partition(tsid)):
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

@ts_manager
def send_areas_to_db(ora_con):
    """save new instances to current session"""
    data = []

    for area in Area:
        if area.is_new:
            for _hd in area.hour_data:
                data.append((
                    _hd.hour, area.code, _hd.losses, _hd.load_losses
                ))

    with ora_con.cursor() as curs:
        curs.execute('''DELETE from rastr_area
                        where loading_protocol is null''')

        curs.executemany('''
            INSERT into rastr_area (hour, o$na, o$dp, o$dp_nag)
            values (:1, :2, :3, :4)
        ''', data)
