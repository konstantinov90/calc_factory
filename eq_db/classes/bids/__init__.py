import time
from utils import DB, ORM
from utils.progress_bar import update_progress
from operator import itemgetter
from sql_scripts import bid_init_script as bis
from sql_scripts import bid_hour_script as bhs
from sql_scripts import bid_pair_script as bps
from .bids import Bid
from .bids_max_price import BidMaxPrice


HOURCOUNT = 24


def make_bids(tsid={}, tdate=''):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}

    print('making bids%s' % ((' for date %s' % tdate) if tdate else ''))
    start_time = time.time()

    con = DB.OracleConnection()

    @DB.process_cursor(con, bis, tsid)
    def process_bid_init(new_row, bids_list):
        bids_list.add_bid(new_row)

    @DB.process_cursor(con, bhs, tsid)
    def process_bid_hour(new_row, bids_list):
        dpg_id = new_row[bhs['dpg_id']]
        bid = bids_list[dpg_id]
        if bid:
            bid.add_hour_data(new_row)

    @DB.process_cursor(con, bps, tsid)
    def process_bid_pair(new_row, bids_list):
        dpg_id = new_row[bps['dpg_id']]
        bid = bids_list[dpg_id]
        if bid:
            bid.add_intervals_data(new_row)

    def calc_max_price(bids_list):
        dummy = [0] * HOURCOUNT
        for bid in bids_list:
            dummy = list(map(lambda hd: max(max(list(map(lambda interval: interval.price, hd.intervals.values()))+[0]), dummy[hd.hour]), bid.hours.values()))
        for hour, max_price in enumerate(dummy):
            ORM.session.add(BidMaxPrice(hour, max_price))

    bids = BidsList()
    process_bid_init(bids)
    process_bid_hour(bids)
    process_bid_pair(bids)
    calc_max_price(bids)

    for i, bid in enumerate(bids):
        bid.serialize(ORM.session)
        update_progress((i + 1) / len(bids))
    ORM.session.commit()

    print('%s %i seconds %s' % (15 * '-', time.time() - start_time, 15 * '-'))
    return bids


class BidsList(object):
    def __init__(self):
        self.bids_list = []
        self.bids_list_index = {}

    def add_bid(self, bis_row):
        dpg_id = bis_row[bis['dpg_id']]
        self.bids_list_index[dpg_id] = len(self.bids_list)
        self.bids_list.append(Bid(bis_row))

    def __len__(self):
        return len(self.bids_list)

    def __getitem__(self, item):
        if item in self.bids_list_index.keys():
            return self.bids_list[self.bids_list_index[item]]
        else:
            return None

    def __iter__(self):
        for n in self.bids_list:
            yield n
