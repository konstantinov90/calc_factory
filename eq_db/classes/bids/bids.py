"""Class Bid."""
from operator import attrgetter
from utils.subscriptable import subscriptable
from utils.printer import display
from ..meta_base import MetaBase
from .bids_hour_data import BidHour
# from .bids_max_price import BidMaxPrice

HOURCOUNT = 24


class Bid(object, metaclass=MetaBase):
    """class bid"""
    max_prices = [[] for hour in range(HOURCOUNT)]
    def __init__(self, bis_row, is_new=False):
        self.dpg_code, self.dpg_id, self.bid_id = bis_row
        self.is_new = is_new
        self.hour_data = {}
        self.hours_index = {}
        self._init_on_load()

    # key = 'dpg_id'
    # lst = {'dpg_id': {}, 'dpg_code': {}}
    def _init_on_load(self):
        """additional initialization"""
        if self.dpg_id not in self.lst.keys():
            self.lst[self.dpg_id] = self
        else:
            self.lst[self.dpg_id] = self
            display('%s bid reset!' % self.dpg_code)
        # if self.dpg_code not in self.lst['dpg_code'].keys():
        #     self.lst['dpg_code'][self.dpg_code] = self
        # else:
        #     display('%s bid reset!' % self.dpg_code)

    def __repr__(self):
        return '<Bid %s>' % self.dpg_code

    def __getitem__(self, item):
        if item in self.hours_index.keys():
            return self.hour_data[self.hours_index[item]]
        else:
            return None

    # @subscriptable
    # @staticmethod
    # def by_dpg_id(item):
    #     """get instance by dpg_id"""
    #     return Bid[item]

    # @subscriptable
    # @staticmethod
    # def by_dpg_code(item):
    #     """get instance by dpg_code"""
    #     return Bid['dpg_code', item]

    def __iter__(self):
        for bid_hour in sorted(self.hour_data.values(), key=attrgetter('hour')):
            yield bid_hour

    def add_hour_data(self, bhs_row):
        """add BidHour instance"""
        hour = bhs_row.hour
        bid_hour_id = bhs_row.bid_hour_id
        self.hours_index[hour] = bid_hour_id
        if bid_hour_id in self.hour_data.keys():
            raise Exception('tried to add same hour to bid %s!' % self.dpg_code)
        self.hour_data[bid_hour_id] = BidHour(bhs_row)

    # def set_max_price(self):
    #     """pass max price to BidHour instance"""
    #     for bid_hour in self.hour_data.values():
    #         bid_hour.set_max_price(self.max_price_list[bid_hour.hour])

    def add_intervals_data(self, bps_row):
        """add interval data to BidHour instance"""
        bid_hour_id = bps_row.bid_hour_id
        self.hour_data[bid_hour_id].add_interval(bps_row)

    # @staticmethod
    # def get_bid_hour_max_price(bid_hour_data, max_prices):
    #     return max(bid_hour_data.get_hour_max_price(), max_prices[bid_hour_data.hour])
    #
    # @classmethod
    # def set_max_price(cls):
    #     """set max_prices_list class attribute"""
    #     dummy = [0] * HOURCOUNT
    #     for bid in cls:s
    #         dummy = [cls.get_bid_hour_max_price(bid_hour_data, dummy) for bid_hour_data in bid]
    #
    #     for hour, max_price in enumerate(dummy):
    #         cls.max_prices[hour] = BidMaxPrice(hour, max_price)
    #
    #     for bid in cls:
    #         for bid_hour_data in bid:
    #             bid_hour_data.set_max_price(cls.max_prices[bid_hour_data.hour])
