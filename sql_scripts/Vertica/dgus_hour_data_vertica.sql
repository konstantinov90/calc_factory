select
hour,
dgu_code,
case when is_blocked = 1 then p else 0 end as pmin,
p as pmax,
case when is_blocked = 1 then p else 0 end as pmin_agg,
p as pmax_agg,
case when is_blocked = 1 then p else 0 end as pmin_tech,
p as pmax_tech,
case when is_blocked = 1 then p else 0 end as pmin_tech,
p as pmax_heat,
case when is_blocked = 1 then p else 0 end as pmin_so,
p as pmax_so,
p as p,
wmax,
wmin,
vgain,
vdrop,
node
from (
select gu.hour,
gu.is_blocked,
gu.dgu_code,
nvl(gaes.vg_gu_pdg, gu.volume) as p,
0 as wmax,
0 as wmin,
gu.volume as vgain,
gu.volume as vdrop,
gu.node
from (
select *
from
(
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
)h,
dm_opr.model_gu_ts gu
where gu.inout = 1
and gu.scenario_fk = :scenario
) gu
left join dm_opr.MODEL_GEN_FIX_TS gaes
on gaes.dgu_Code = gu.dgu_code
and gaes.scenario_fk = gu.scenario_fk
and gaes.hour = gu.hour
)a
