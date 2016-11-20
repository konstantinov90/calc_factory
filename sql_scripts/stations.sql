select trader_id, trader_code, station_type, station_category
from tsdb2.wh_trader partition (&tsid)
where trader_type = 102
