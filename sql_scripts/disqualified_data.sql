select gtp.trader_id, gtp.trader_code, volume_without_losses fed_station_cons, volume_gen_conn_fs attached_supplies_gen
from tsdb2.wh_MINIWH_DisqualFS partition (&tsid) m,
tsdb2.wh_trader partition (&tsid) gtp
where gtp.trader_code = m.dpg_code
