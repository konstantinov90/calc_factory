select bi.dpg_code, bip.dpg_id, bip.bid_direction,
bih.hour, bip.interval_num, nvl(price,0) price, nvl(volume,0)
from tsdb2.wh_bid_init partition (&tsid) bi, tsdb2.wh_bid_init_pair partition (&tsid) bip,
tsdb2.wh_bid_init_hour partition (&tsid) bih
where bi.bid_id = bih.bid_id
and bih.bid_hour_id = bip.bid_hour_id
and bip.status = 1
--and bi.dpg_id = :dpg_id
--and bi.dpg_code = 'PSAYANAL'
--order by 4, 5
