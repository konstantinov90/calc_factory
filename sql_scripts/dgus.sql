select trader_id dgu_id, to_number(trader_code) dgu_code, parent_object_id dpg_id, fixed_power
from tsdb2.wh_trader partition (&tsid)
where trader_type = 103
