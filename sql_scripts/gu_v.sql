select gu_code code, dgu_code dgu_id, '' fuel_type_list, volume fixed_power
from dm_opr.model_gu_ts
where scenario_fk = :scenario
and inout = :inout
