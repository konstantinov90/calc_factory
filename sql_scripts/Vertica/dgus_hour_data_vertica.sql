select
gu.hour,
gu.dgu_code,
pmin,
pmax,
pminagg,
pmax as pmax_agg,
pmin as pmin_tech,
pmax as pmax_tech,
pmin as pmin_tech,
pmax pmax_heat,
pmin pmin_so,
pmax as pmax_so,
nvl(vg_gu_pdg, pmax) as p,
0 as wmax,
0 as wmin,
vgain,
vdrop,
node
from (
	select scenario_fk, hour, dgu_code, sum(pmin) pmin, sum(pmax) pmax, sum(pmint) pminagg,
	sum(vgain) vgain, sum(vdrop) vdrop, node
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
	group by hour, dgu_code, node, scenario_fk
) gu
left join dm_opr.MODEL_GEN_FIX_TS gaes
on gaes.dgu_Code = gu.dgu_code
and gaes.scenario_fk = gu.scenario_fk
and gaes.hour = gu.hour
where nvl(gaes.is_new, 1) = 1
order by dgu_code, hour
