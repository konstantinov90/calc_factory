select trader_code, trader_id, price_zone_code, is_gaes, is_blocked,
is_unpriced_zone, ownneeds_dpg_id fed_station_id, dpg_station_id station_id,
is_spot_trader, parent_dpg_id, region_code
from tsdb2.wh_trader partition (&tsid)
where trader_type = 100
and dpg_type = 2
and is_impex = 0

-- select st.trader_code station_Code, st.station_type, gtp.trader_code dpg_code,
-- gtp.trader_id gtp_id, /*to_number(rge.trader_code) rge_code, */gtp.price_zone_code,
-- --ga.ga_pnt_code, ga.fuel_type_list,
-- max(case when to_char(7) in ga.fuel_type_list then 1 else 0 end) is_pintsch_gas,
-- gtp.is_gaes, gtp.is_blocked, gtp.is_unpriced_zone, gtp.ownneeds_dpg_id fed_station_id
-- from tsdb2.wh_trader partition (&tsid) gtp, tsdb2.wh_trader partition (&tsid) rge,
-- tsdb2.wh_trader partition (&tsid) st, tsdb2.wh_trader partition (&tsid) ga
-- where rge.parent_object_id = gtp.trader_id
-- and rge.trader_type = 103
-- and st.trader_id = gtp.dpg_station_id
-- --and gtp.is_blocked = 0
-- and ga.parent_object_id = rge.trader_id
-- and gtp.is_unpriced_zone = 0
-- --and gtp.is_gaes = 0
-- group by st.trader_code, st.station_type, gtp.trader_code, gtp.trader_id, gtp.price_zone_code, gtp.is_gaes,
--          gtp.is_blocked, gtp.is_unpriced_zone, gtp.ownneeds_dpg_id
