select bip.bid_hour_id, interval_num interval_number, nvl(price, 0) price,
nvl(volume, 0) volume, bip.dpg_id, volume_src0 volume_init
from tsdb2.wh_bid_init_pair partition (&tsid) bip--, tsdb2.wh_bid_init_hour partition (&tsid) bih
where status = 1
-- and bih.bid_hour_id = bip.bid_hour_id
-- and bih.hour = 0
