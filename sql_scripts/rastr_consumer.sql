select hour, o$num consumer_code, o$type type, o$pdem pdem
from tsdb2.wh_rastr_consumer2 partition (&tsid)