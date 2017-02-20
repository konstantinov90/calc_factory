"""Create BidMaxPrice instances."""
from utils.trade_session_manager import ts_manager
from .bids_max_price import BidMaxPrice
from ..bids import Bid

HOURCOUNT = 24

@ts_manager
def make_bid_max_prices():
    """create BidMaxPrice instances"""
    dummy = [(hour, 0) for hour in range(HOURCOUNT)]
    for bid in Bid:
        dummy = [(bhd.hour, max(bhd.get_hour_max_price(), dummy[bhd.hour][1]))
                 for bhd in bid]

    for hour, max_price in dummy:
        BidMaxPrice(hour, max_price)
