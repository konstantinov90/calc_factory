select hour, o$num consumer_code, o$type type, o$pdem pdem, o$pdsready
from tsdb2.wh_rastr_consumer2 partition (&tsid)
-- where hour = 0
