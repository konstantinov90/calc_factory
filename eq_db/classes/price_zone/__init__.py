import time
from itertools import product
from utils import ORM
from utils.progress_bar import update_progress

from .price_zone import PriceZone

HOURCOUNT = 24
price_zones = (1, 2)

def make_price_zones(tsid={}, tdate=''):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}
    print('making price_zones%s' % ((' for date %s' % tdate) if tdate else ''))

    start_time = time.time()

    price_zones_lst = []

    for hour, pz in product(range(HOURCOUNT), price_zones):
        price_zones_lst.append(PriceZone(hour, pz))

    for i, price_zone in enumerate(price_zones_lst):
        price_zone.serialize(ORM.session)
        update_progress((i + 1) / len(price_zones_lst))

    ORM.session.commit()

    print('%s %s seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))

    return price_zones_lst
