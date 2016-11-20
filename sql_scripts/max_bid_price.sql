select hour, bid_max_price
from tsdb2.wh_hour partition (&tsid)
