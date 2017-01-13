"""Class BidHour."""
from operator import attrgetter
from .bids_hour_interval_data import BidHourInterval


class BidHour(object):
    """class BidHour"""
    def __init__(self, bhs_row):
        self.bid_id, self.bid_hour_id, self.hour, _ = bhs_row
        self.intervals = {}
        # self.max_price = None

    @property
    def interval_data(self):
        """interval_data property"""
        return sorted(self.intervals.values(), key=attrgetter('interval_number'))

    def __repr__(self):
        return '<BidHour %i>' % self.hour

    # def set_max_price(self, max_price):
    #     """set BidMaxPrice instance"""
    #     self.max_price = max_price

    def add_interval(self, bps_row):
        """add BidHourInterval instance"""
        interval_number = bps_row.interval_number
        if interval_number in self.intervals.keys():
            raise Exception('tried to add same interval twice to %r!' % self.bid_hour_id)
        self.intervals[interval_number] = BidHourInterval(bps_row)

    def get_hour_max_price(self):
        return self.interval_data[-1].price if self.interval_data else 0
