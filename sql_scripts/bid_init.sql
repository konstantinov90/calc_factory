select bi.dpg_code, bi.dpg_id, bi.bid_id
from tsdb2.wh_bid_init partition (&tsid) bi
