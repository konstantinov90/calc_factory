"""Class BidHourInterval."""


class BidHourInterval(object):
    """class BidHourInterval"""
    def __init__(self, bps_row):
        self.bid_hour_id, self.interval_number, self.price, \
        self.volume, _, self.volume_init = bps_row

    def __repr__(self):
        return '<BidHourInterval %i: %f @ %f>' % (self.interval_number, self.volume, self.price)
