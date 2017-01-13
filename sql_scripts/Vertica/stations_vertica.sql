select distinct
station_pk as TRADER_ID,
station_code as TRADER_CODE,
station_type_fk as STATION_TYPE,
STATION_CATEGORY_FK as STATION_CATEGORY
from dm_opr.model_gu_ts
where inout = 1
and is_added = 1
and scenario_fk = :scenario
