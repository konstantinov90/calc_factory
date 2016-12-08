select hour, o$na area, o$dp losses, o$dp_nag load_losses
from tsdb2.wh_rastr_area partition (&tsid)
-- where hour = 0
