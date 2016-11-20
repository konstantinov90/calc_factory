select dpg_code, dpg_code dpg_id, null price_zone_code, 0 is_gaes, 0 is_blocked,
0 is_unpriced_zone, null fed_station_id, station_pk station_id
from dm_opr.model_gu_ts
where scenario_fk = :scenario
and inout = 1
