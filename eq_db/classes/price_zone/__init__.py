"""Create PriceZone instances."""
from itertools import product
from utils.trade_session_manager import ts_manager
from .price_zone import PriceZone

HOURCOUNT = 24
PRICE_ZONES = (1, 2)


@ts_manager
def make_price_zones(*args):
    """create PriceZone instances"""
    PriceZone.clear()

    for hour, price_zone_code in product(range(HOURCOUNT), PRICE_ZONES):
        PriceZone(hour, price_zone_code)
