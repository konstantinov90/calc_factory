select distinct
dpg_code,
dpg_pk || '_' || hour as bid_hour_id,
hour,
dpg_pk
from dm_opr.MODEL_DPGC_BID_TS
where scenario_fk = :scenario
union ALL
select distinct
dpg_code,
gu_code || '_' || hour as bid_hour_id,
hour,
gu_code
from dm_opr.MODEL_BID_GEN_TS
where scenario_fk = :scenario
