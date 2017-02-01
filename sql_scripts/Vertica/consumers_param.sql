SELECT hour, consumer_code, type, pdem, 0 as pdsready
FROM dm_opr.MODEL_CONSUMER_PAR_TS
where scenario_fk = :scenario
