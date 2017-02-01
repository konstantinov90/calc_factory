select to_number(n$num) rge_code, f$price price,
nvl(1000000 + o$numgr, DENSE_RANK() OVER (ORDER BY n$hour_start, to_number(n$num), f$price)) integral_id,
n$hour_start hour_start, n$hour_end hour_end, f$volume_min min_volume, f$volume volume,
o$numgr
from tsdb2.wh_wsumgen partition (&tsid)
