select distinct
dpg_pk,
dpg_pk || hour as bid_hour_id,
hour,
dpg_pk
from dm_opr.MODEL_DPGC_BID_TS
where scenario_fk = :scenario
union ALL
select distinct
gu_code,
gu_code || hour as bid_hour_id,
hour,
gu_code
from dm_opr.MODEL_BID_GEN_TS
where scenario_fk = :scenario
