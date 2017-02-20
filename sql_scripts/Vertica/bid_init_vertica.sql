select distinct
dpg_code,
dpg_pk as dpg_id,
dpg_pk as bid_id
from dm_opr.MODEL_DPGC_BID_TS
where scenario_fk = :scenario

union ALL

select distinct
dpg_code,
nvl(gu_code, dpg_pk) as dpg_id,
nvl(gu_code, dpg_pk) as bid_id
from dm_opr.MODEL_BID_GEN_TS
where scenario_fk = :scenario

union all

select distinct
dpg_code,
dpg_pk as dpg_id,
dpg_pk as bid_id
from dm_opr.MODEL_DPGG_BID_TS
where scenario_fk = :scenario
