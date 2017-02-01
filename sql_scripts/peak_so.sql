select PRICE_ZONE_CODE, HOUR_NUM_START, HOUR_NUM_END
from tsdb2.wh_peak_so partition (&tsid) pso
where pso.peak_month = trunc(:tdate, 'mm')
