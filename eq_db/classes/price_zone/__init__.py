"""Create PriceZone instances."""
from itertools import product
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import price_zone_script as pzs
from .price_zone import PriceZone

HOURCOUNT = 24
PRICE_ZONES = (1, 2)


@ts_manager
def make_price_zones(tsid):
    """create PriceZone instances"""
    con = DB.OracleConnection()
    PriceZone.clear()

    for new_row in con.script_cursor(pzs, tsid=DB.Partition(tsid)):
        price_zone = PriceZone(new_row)
        for hour in range(HOURCOUNT):
            price_zone.add_price_zone_hour_data(hour)
