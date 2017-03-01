select distinct
gu_code
from dm_opr.model_gu_ts
where inout = 0
and scenario_fk = :scenario
