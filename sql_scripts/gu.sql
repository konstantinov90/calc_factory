select to_number(ga_pnt_code) code, parent_object_id dgu_id, fuel_type_list, fixed_power
from tsdb2.wh_trader partition (&tsid)
where trader_type = 104
