select 0 as hour, node_code_from node_from, node_code_to node_to, par_count n_par,
0 as state, r, x, b, 0 kt_re, 0 kt_im, 0 div, g, 0 as type, 0 b_from, 0 b_to, 0 area, 0 losses
from dm_opr.model_branch_ts
where scenario_fk = :scenario
and inout = 1
