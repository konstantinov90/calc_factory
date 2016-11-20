select o$npoint - 25 hour, o$num group_code, o$sta state, o$pmin p_min,
o$pmax p_max
from tsdb2.wh_rastr_preserves partition (&tsid)
where hour = 0