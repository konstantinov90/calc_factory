import time
from utils import DB
from operator import itemgetter
from sql_scripts import BidInitsScript
from sql_scripts import BidHoursScript
from sql_scripts import BidPairsScript


HOURCOUNT = 24

bis = BidInitsScript()
bhs = BidHoursScript()
bps = BidPairsScript()


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

    bids = BidsList()
    process_bid_init(bids)
    process_bid_hour(bids)
    process_bid_pair(bids)

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


class Bid(object):
    def __init__(self, bis_row):
        self.dpg_code = bis_row[bis['dpg_code']]
        self.dpg_id = bis_row[bis['dpg_id']]
        self.bid_id = bis_row[bis['bid_id']]
        self.hour_data = {}
        self.hour_data_index = {}

    def __getitem__(self, item):
        if item in self.hour_data_index.keys():
            return self.hour_data[self.hour_data_index[item]]
        else:
            return None

    def __iter__(self):
        for n in sorted(self.hour_data.keys()):
            yield self.hour_data[n]

    def add_hour_data(self, bhs_row):
        hour = bhs_row[bhs['hour']]
        bid_hour_id = bhs_row[bhs['bid_hour_id']]
        self.hour_data_index[hour] = bid_hour_id
        if bid_hour_id in self.hour_data.keys():
            raise Exception('tried to add same hour to bid %s!' % self.dpg_code)
        self.hour_data[bid_hour_id] = []


    def add_intervals_data(self, bps_row):
        bid_hour_id = bps_row[bps['bid_hour_id']]
        self.hour_data[bid_hour_id].append(bps_row)
        self.hour_data[bid_hour_id] = sorted(self.hour_data[bid_hour_id], key=itemgetter(bps['interval_number']))
