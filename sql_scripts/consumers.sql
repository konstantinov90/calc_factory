select
es_ref_ex,
to_number(consumer2),
es_ref area,
gtp.dpg_dr_volume_decr_fact,
gtp.dpg_dr_hours_decr,
price_zone_code,
trader_code dpg_code,
is_system,
is_guarantee_supply_co,
trader_id dpg_id,
fed_station is_fed_station,
is_disqualified,
is_unpriced_zone,
is_fsk,
p.min_value min_forecast,
p.max_value max_forecast,
is_spot_trader,
region_code,
parent_object_id
from tsdb2.wh_trader partition (&tsid) gtp
left outer join tsdb2.wh_tv_prognoz_min_max partition (&tsid) p
on gtp.trader_code = p.dpg_code
where dpg_type = 1
and is_impex = 0
--and trader_code = 'PSAYANAL'
