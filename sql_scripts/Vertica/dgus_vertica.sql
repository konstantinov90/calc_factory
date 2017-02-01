select
dgu_code as dgu_id,
dgu_code,
nvl(dpg_pk, gu_code) as dpg_id,
sum(volume) as fixed_power
from dm_opr.model_gu_ts
where inout = 1
and scenario_fk = :scenario
group by dgU_code, nvl(dpg_pk, gu_code)
