SELECT pnt_dpgc_code as consumer2,
area,
null as dem_rep_fact,
null as dem_rep_hours,
1 as pz,
dpg_code,
is_system,
is_guarantee_supply_co,
dpg_id,
is_fed_station,
is_disqualified,
is_unpriced_zone,
is_fsk,
min_forecast,
max_forecast,
is_spot_trader,
REGION_CODE
FROM dm_opr.MODEL_CONSUMERS_TS
where scenario_fk = :scenario
and (is_fsk = 1
or is_system = 1)
