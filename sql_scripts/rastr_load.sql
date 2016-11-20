select hour, o$node node_code, o$consumer consumer_code, o$p pn, o$nodedose node_dose, o$nodestate node_state
from tsdb2.wh_rastr_load partition (&tsid)
