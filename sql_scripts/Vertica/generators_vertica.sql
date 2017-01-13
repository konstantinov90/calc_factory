select
DPG_CODE,
gu_CODE,
PZ,
is_gaes,
is_blocked,
upz,
AUX_DPG_FK,
STATION_PK,
1 is_spot_trader,
null parent_dpg_id
from dm_opr.model_gu_ts
where scenario_fk = :scenario
and inout = 1
