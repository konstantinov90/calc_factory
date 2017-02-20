select
node_code_from,
node_code_to,
branch_num
from dm_opr.MODEL_BRANCH_TS b
where scenario_fk = :scenario
and inout = 0
