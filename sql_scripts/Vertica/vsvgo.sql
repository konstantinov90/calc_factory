select
to_number(gu_pnt_code),
hour,
state
from dm_opr.modeL_vsvgo_ts
where scenario_fk = :scenario
