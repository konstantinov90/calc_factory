select
dpg_pk || hour as bid_hour_id,
step,
price,
volume,
dpg_pk as dpg_id,
volume as volume_init
from dm_opr.MODEL_DPGC_BID_TS
where scenario_fk = :scenario
union ALL
select
gu_code || hour as bid_hour_id,
step,
price,
volume,
gu_code as dpg_id,
volume as volume_init
from dm_opr.MODEL_BID_GEN_TS
where scenario_fk = :scenario
