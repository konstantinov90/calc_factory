select bid_id, bid_hour_id, hour, dpg_id
from tsdb2.wh_bid_init_hour partition (&tsid)
-- where hour = 0
