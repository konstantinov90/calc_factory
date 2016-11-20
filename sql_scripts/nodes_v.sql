select 0 as hour, node_pk node_code, 0 area, 0 state, 1 as type, voltage nominal_voltage,
0 pn, 0 qmax, 0 qmin, 0 voltage, 0 phase, 0 fixed_voltage,
0 g_shunt, 0 b_shunt, 0 min_voltage, 0 max_voltage,
0 qn, 0 qg, 0 pg, 0 q_shunt, null price_zone, 0 p_shunt, 0 b_shr
from dm_opr.model_node_ts
where scenario_fk = :scenario
