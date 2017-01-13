select
n.node_pk as node_code,
n.EA_CODE as area,
n.voltage,
0.8*n.voltage as min_voltage,
1.2*n.voltage as max_voltage,
n.pz as price_zone,
h.hour,
n.state,
n.type,
0 as pn,
0 as qmax,
0 as qmin,
n.voltage,
0 as phase,
n.voltage as fixed_voltage,
0 as g_shunt,
0 as b_shunt,
0 as qn,
0 as qg,
0 as pg,
0 as q_shunt,
0 as p_shunt,
0 as b_shr
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
dm_opr.model_node_ts n
where scenario_fk = :scenario
