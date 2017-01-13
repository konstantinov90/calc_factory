select distinct
dpg_code,
dpg_pk as dpg_id,
dpg_code as bid_id
from dm_opr.MODEL_DPGC_BID_TS, ods_002.wh_trader t, ods_002.trade_Session ts
where scenario_fk = 1
and t.trade_session_id = ts.trade_session_id
and ts.target_date = :target_date
and is_main = 1
and t.trader_id = dpg_pk
and t.fed_station = 0
and t.is_gaes = 0
union ALL
select distinct
dpg_code,
gu_code as dpg_id,
dpg_code as bid_id
from dm_opr.MODEL_BID_GEN_TS
where scenario_fk = :scenario
