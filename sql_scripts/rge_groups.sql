select o$numgr group_code, o$numrge rge_code
from tsdb2.wh_rastr_rgegroup partition (&tsid)
where hour = 0