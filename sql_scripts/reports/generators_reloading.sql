select rge.target_date, participant_code, station_code, name, oes, station_type,
sum(old_pmin) pmin_old, sum(old_pmax) pmax_old, decode(sum(old_tg), 0, sum(nvl(ddh_old.volume,0))/count(*), sum(old_tg)) tg_old,
sum(new_pmin) pmin_new, sum(new_pmax) pmax_new, decode(sum(new_tg), 0, sum(nvl(ddh_new.volume,0))/count(*), sum(new_tg)) tg_new,
nvl(sum(new_pmin),0) - nvl(sum(old_pmin),0) dif_pmin,
nvl(sum(new_pmax),0) - nvl(sum(old_pmax),0) dif_pmax,
nvl(sum(new_tg),0) - nvl(sum(old_tg),0) dif_tg
from (select dpg_code, sum(volume)/1000 volume from wh_deal_data_hour where trade_session_id = :tsid_init and deal_type = 3 group by dpg_code) ddh_old
right join tmp_sipr_report_generators_rge rge
on rge.gtp_code = ddh_old.dpg_code
left join (select dpg_code, sum(volume)/1000 volume from wh_deal_data_hour where trade_session_id = :tsid and deal_type = 3 group by dpg_code) ddh_new
on rge.gtp_code = ddh_new.dpg_code
where id_init = :tsid_init
and id_new = :tsid
and rge.target_date = :tdate
group by rge.target_date, participant_code, station_code, name, oes, station_type
order by oes, name

-- select nvl(new.begin_date, old.begin_date) target_date,
-- nvl(new.participant_code, old.participant_code) participant_code,
-- nvl(new.station_code, old.station_code) station_code,
-- nvl(old.name, new.name) name, nvl(new.oes, old.oes) oes,
-- nvl(old.station_type, new.station_type) station_type,
-- old.pmin old_pmin, old.pmax old_pmax, old.tg old_tg,
-- new.pmin new_pmin, new.pmax new_pmax, new.tg new_tg,
-- nvl(new.pmin,0) - nvl(old.pmin,0) pmin_diff, nvl(new.pmax,0) - nvl(old.pmax,0) pmax_diff,
-- nvl(new.tg,0) - nvl(old.tg,0) tg_diff
-- from (
--     select st.begin_date, prt.trader_code participant_code, st.trader_code station_code,
--     nvl(st.full_name, max(gtp.full_name)) name, avg(gtp.oes) oes,
--     decode(st.station_type, 1, 'ТЭС', 2, 'ГЭС', 3, 'АЭС',
--         4, 'ТЭС', 5, 'СЭС', 6, 'ВЭС') station_type,
--     nvl(sum(ddh.volume /1000),0) tg, sum(o$pmin) pmin, sum(o$pmax) pmax
--     from wh_trader partition (&tsid) st,
--     wh_trader partition (&tsid) prt,
--     wh_deal_data_hour partition (&tsid) ddh, (
--         select gtp.trader_id, gtp.dpg_station_id, gtp.oes,
--         sum(o$pmin) o$pmin, sum(o$pmax) o$pmax, rg.hour, gtp.full_name
--         from wh_trader partition (&tsid) rge,
--         wh_rastr_generator partition (&tsid) rg,
--         wh_trader partition (&tsid) gtp
--         where gtp.trader_id = rge.parent_object_id
--         and rg.o$num = rge.trader_code
--         and rge.trader_type = 103
--         group by gtp.trader_id, gtp.dpg_station_id, rg.hour,
--         gtp.oes, gtp.full_name
--     ) gtp
--     where st.trader_id = gtp.dpg_station_id
--     and ddh.hour(+) = gtp.hour
--     and gtp.trader_id = ddh.dpg_id(+)
--     and ddh.deal_type(+) = 3
--     and st.parent_object_id = prt.trader_id
--     group by st.begin_date, st.trader_code,
--     prt.trader_code, st.full_name,
--     st.station_type
-- ) new full join (
--     select st.begin_date, prt.trader_code participant_code, st.trader_code station_code,
--     nvl(st.full_name, max(gtp.full_name)) name, avg(gtp.oes) oes,
--     decode(st.station_type, 1, 'ТЭС', 2, 'ГЭС', 3, 'АЭС',
--         4, 'ТЭС', 5, 'СЭС', 6, 'ВЭС') station_type,
--     nvl(sum(ddh.volume /1000),0) tg, sum(o$pmin) pmin, sum(o$pmax) pmax
--     from wh_trader partition (&tsid_init) st,
--     wh_trader partition (&tsid_init) prt,
--     wh_deal_data_hour partition (&tsid_init) ddh, (
--         select gtp.trader_id, gtp.dpg_station_id, gtp.oes,
--         sum(o$pmin) o$pmin, sum(o$pmax) o$pmax, rg.hour, gtp.full_name
--         from wh_trader partition (&tsid_init) rge,
--         wh_rastr_generator partition (&tsid_init) rg,
--         wh_trader partition (&tsid_init) gtp
--         where gtp.trader_id = rge.parent_object_id
--         and rg.o$num = rge.trader_code
--         and rge.trader_type = 103
--         group by gtp.trader_id, gtp.dpg_station_id, rg.hour,
--         gtp.oes, gtp.full_name
--     ) gtp
--     where st.trader_id = gtp.dpg_station_id
--     and ddh.hour(+) = gtp.hour
--     and gtp.trader_id = ddh.dpg_id(+)
--     and ddh.deal_type(+) = 3
--     and st.parent_object_id = prt.trader_id
--     group by st.begin_date, st.trader_code,
--     prt.trader_code, st.full_name,
--     st.station_type
-- ) old
-- on new.station_code = old.station_code
-- and new.participant_code = old.participant_code
-- order by nvl(new.oes, old.oes), nvl(old.name, new.name)
