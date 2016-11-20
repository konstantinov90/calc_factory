select hour, o$ns section_code, o$pmax p_max, o$pmin p_min,
o$sta, o$tip
from tsdb2.wh_rastr_sechen partition (&tsid)
