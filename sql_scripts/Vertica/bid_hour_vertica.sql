select bid_id, dpg_id || hour as bid_hour_id, hour, dpg_id
from (
	select distinct
	dpg_code,
	dpg_pk as dpg_id,
	dpg_pk as bid_id
	from dm_opr.MODEL_DPGC_BID_TS
	where scenario_fk = :scenario
) b full join (
  select 0 as hour from dual union all
  select 1 as hour from dual union all
  select 2 as hour from dual union all
  select 3 as hour from dual union all
  select 4 as hour from dual union all
  select 5 as hour from dual union all
  select 6 as hour from dual union all
  select 7 as hour from dual union all
  select 8 as hour from dual union all
  select 9 as hour from dual union all
  select 10 as hour from dual union all
  select 11 as hour from dual union all
  select 12 as hour from dual union all
  select 13 as hour from dual union all
  select 14 as hour from dual union all
  select 15 as hour from dual union all
  select 16 as hour from dual union all
  select 17 as hour from dual union all
  select 18 as hour from dual union all
  select 19 as hour from dual union all
  select 20 as hour from dual union all
  select 21 as hour from dual union all
  select 22 as hour from dual union all
  select 23 as hour from dual
)h
on 1 = 1

union ALL

select distinct
nvl(gu_code, dpg_pk) as bid_id,
nvl(gu_code, dpg_pk) || hour as bid_hour_id,
hour,
nvl(gu_code, dpg_pk) as dpg_id
from dm_opr.MODEL_BID_GEN_TS
where scenario_fk = :scenario
