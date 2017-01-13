select 
h.hour,
gu.dgu_code,
gu.gu_code,
0 as pmin,
gu.volume as pmax,
0 as pmin_t,
gu.volume as pmax_t,
gu.inout as state,
gu.volume as vgain,
gu.volume as vdrop,
0 as is_sysgen,
0 as repair
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
where scenario_fk = 1
