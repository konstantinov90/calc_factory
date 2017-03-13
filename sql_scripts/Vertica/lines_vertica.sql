select
case when flip = 1 then branch.node_code_to else branch.node_code_from end f1,
case when flip = 1 then branch.node_code_from else branch.node_code_to end t1,
branch.branch_num as n_par,
case when flip = 1 then 1/branch.kt else branch.kt end kt,
0 as kt_im,
0 as div,
branch.branch_type,
0 as area,
branch.hour,
0 as state,
branch.R,
branch.X,
branch.B,
branch.G,
0 as B_from,
0 as B_to,
0 as losses
from
(
select
h.hour,
b.node_code_from,
b.node_code_to,
b.branch_num,
0 as state,
b.R,
b.X,
b.B,
coalesce(b.kt,1) as kt,
0 as kt_im,
0 as div,
b.G,
b.branch_type,
0 as B_from,
0 as B_to,
0 as area,
0 as losses,
case when (node_code_from = 532198 and node_code_to = 1200019) or
(node_code_from = 1200220 and node_code_to = 1200122) or
(node_code_from = 510684 and node_code_to = 510685) or
(node_code_from = 1000722 and node_code_to = 1000793)
then 1 else 0 end as flip
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
dm_opr.MODEL_BRANCH_TS b
where scenario_fk = :scenario
and inout = 1
)branch
