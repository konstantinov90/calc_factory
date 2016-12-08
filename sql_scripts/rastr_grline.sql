select hour, o$ns section_code, o$ip node_from_code, o$iq node_to_code, o$sta state, o$dv div
from tsdb2.wh_rastr_grline partition (&tsid)
-- where hour = 0
