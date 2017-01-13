select
gu_code,
gu_code,
dgu_code,
fuel_type_fk,
volume
from dm_opr.model_gu_ts
where inout = 1
and scenario_fk = 1
