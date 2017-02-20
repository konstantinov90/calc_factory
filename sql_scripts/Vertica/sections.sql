SELECT hour, section_code, p_max, p_min,
1 as sta,
0 as type
FROM dm_opr.MODEL_SECTION_TS
where scenario_fk = :scenario
