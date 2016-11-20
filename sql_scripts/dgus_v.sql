select dgu_code dgu_id, dgu_code, dpg_code dpg_id, volume fixed_power
from dm_opr.model_gu_ts
where scenario_fk = :scenario
and inout = 1
