select station_pk trader_id, station_code, 1 station_type, null station_category
from dm_opr.model_gu_ts
where scenario_fk = :scenario
and inout = 1
