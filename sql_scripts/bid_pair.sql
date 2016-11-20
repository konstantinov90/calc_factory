select bid_hour_id, interval_num interval_number, nvl(price, 0) price,
nvl(volume, 0) volume, bid_direction, dpg_id, nvl(volume_src0, 0) volume_init
from tsdb2.wh_bid_init_pair partition (&tsid)
where status = 1
