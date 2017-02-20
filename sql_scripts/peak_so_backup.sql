select PRICE_ZONE_CODE, HOUR_NUM_START, HOUR_NUM_END
from (
    select pso.*, max(peak_month) over (partition by 1) max_peak_month
    from tsdb2.peak_so@tseur1 pso
)
where peak_month = max_peak_month
