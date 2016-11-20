select hour, o$ny node_code, o$na area, o$sta state, o$tip type, o$uhom nominal_voltage,
o$pn pn, o$qmax qmax, o$qmin qmin, o$vras voltage, o$delta phase, o$vzd fixed_voltage,
o$gsh g_shunt, o$bsh b_shunt, o$umin min_voltage, o$umax max_voltage,
o$qn qn, o$qg qg, o$pg pg, o$qsh q_shunt, price_zone_mask/2 price_zone, o$psh p_shunt, o$bshr b_shr
from tsdb2.wh_rastr_node partition (&tsid)
