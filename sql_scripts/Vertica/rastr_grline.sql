SELECT hour, section_code, node_code_from, node_code_to, state, div
FROM (
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
)h, dm_opr.MODEL_GR_BRANCH_TS gr
where scenario_fk = :scenario
