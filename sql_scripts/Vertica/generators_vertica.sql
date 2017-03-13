select distinct
DPG_CODE,
nvl(dpg_pk, gu_CODE) as dpg_id,
PZ as price_zone_code,
nvl(is_gaes,0),
nvl(is_blocked,0),
nvl(upz,0) as is_unpriced_zone,
AUX_DPG_FK as fed_station_id,
STATION_PK as station_id,
1-nvl(is_blocked, 0) as is_spot_trader,
PARENT_DPG_FK as parent_dpg_id,
to_number(region_fk) as region_code,
nvl(participant_pk, 987654321) participant_id,
ips_fk oes,
rtrim(station_name)
from dm_opr.model_gu_ts
where scenario_fk = :scenario
and inout = 1
