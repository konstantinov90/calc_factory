select trader_code dpg_code, trader_id dpg_id, is_disqualified, is_unpriced_zone,
section_number, dpg_type direction, is_unpriced_zone
from tsdb2.wh_trader partition (&tsid) gtp
where is_impex = 1