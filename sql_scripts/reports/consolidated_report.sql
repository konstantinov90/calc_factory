/* Formatted on 2013/08/20 17:29 (Formatter Plus v4.8.7) */
WITH ts AS
     (
        SELECT trade_session_id, :pz price_zone_code, target_date targ_date, note
          FROM tsdb2.trade_session
         where trade_session_id = :tsid
--          and note = 'Готово!!! Модельные расчеты по транзиту Иркутск - Красноярск'
          order by target_date
           )
select * from (
 select t1.targ_date,
    decode (oes, 0, 'Экспорт', 999, decode(t1.price_zone_code, 1, 'Первая ценовая зона', 'Вторая ценовая зона'), ot.oes_name) oes_name,
    --decode (SHEET, 1, hour, 2, 'SUM'),
    hour,
    decode (SHEET,1, round(t1.tg_volume, 0),
                 2, sum(round(decode(SHEET,1,t1.tg_volume,0), 0)) over (partition by targ_date, oes)) tg_volume,
    decode (SHEET, 1, t1.rd_volume, 2, /*sum(decode(SHEET, 1, t1.rd_volume,0)) over (partition by oes)*/NULL) rd_volume,
    decode (SHEET, 1, t1.spot_buy_volume, 2, /*sum(decode(SHEET, 1, t1.spot_buy_volume,0)) over (partition by oes)*/NULL) spot_buy_volume,
    decode (SHEET, 1, t1.spot_buy_losses, 2, /*sum(decode(SHEET, 1, t1.spot_buy_losses,0)) over (partition by oes)*/NULL) spot_buy_losses,
--    decode (SHEET, 1, t1.spot_buy_amount,
--                   2, sum(decode(SHEET,1,t1.spot_buy_amount,0)) over (partition by targ_date, oes)) spot_buy_amount,
    decode (SHEET, 1, t1.spot_sell_volume, 2, /*sum(decode(SHEET, 1, t1.spot_sell_volume,0)) over (partition by oes)*/NULL) spot_sell_volume,
    decode (SHEET, 1, t1.spot_sell_losses, 2, /*sum(decode(SHEET, 1, t1.spot_sell_losses,0)) over (partition by oes)*/NULL) spot_sell_losses,
    decode (SHEET,1, round(1000*t1.qc1_gtp_ppp/vc1_gtp_ppp, 2),
                  2, round(sum(decode(SHEET,1,1000*t1.qc1_gtp_ppp,0)) over (partition by targ_date, oes) /
                          sum(decode(SHEET,1,t1.vc1_gtp_ppp,2)) over (partition by targ_date, oes),2)) avg,
    decode(SHEET, 1, round(1000*t1.min_kc1_gtp_ppp, 2),
            2, min(decode(SHEET, 1, round(1000*t1.min_kc1_gtp_ppp, 2), 1E9)) over (partition by targ_date, oes)) min_price,
    decode(SHEET, 1, round(1000*t1.max_kc1_gtp_ppp, 2),
            2, max(decode(SHEET, 1, round(1000*t1.max_kc1_gtp_ppp, 2), 0)) over (partition by targ_date, oes)) max_price
    from
( SELECT decode(grouping(oe.oes), 0, oe.oes, 999) oes,
          case when grouping(ddh.hour) = 0 then ddh.hour else null end hour,
          sum(decode(ddh.deal_type, 3, ddh.volume, 0))*0.001 as tg_volume,
          sum(decode(ddh.deal_type, 15, ddh.volume, 0))*0.001 as rd_volume,
          sum(decode(ddh.deal_type, 1, ddh.volume, 0))*0.001 as spot_buy_volume,
          sum(decode(ddh.deal_type, 1, ddh.losses, 0))*0.001 as spot_buy_losses,
          sum(decode(ddh.deal_type, 1, ddh.amount, 0)) as spot_buy_amount,
          sum(decode(ddh.deal_type, 12, ddh.volume, 0))*0.001 as spot_sell_volume,
          sum(decode(ddh.deal_type, 12, ddh.losses, 0))*0.001 as spot_sell_losses,
          sum(case when ddh.deal_type = 3 and nvl(gtp.is_fsk, 0) <> 1 and nvl(gtp.is_unpriced_zone, 0) = 0 then ddh.volume else 0 end) as vc1_gtp_ppp,
          sum(case when ddh.deal_type = 3 and nvl(gtp.is_fsk, 0) <> 1 and nvl(gtp.is_unpriced_zone, 0) = 0 then ddh.amount else 0 end) as qc1_gtp_ppp,
          max(case when ddh.deal_type = 3 and nvl(gtp.is_fsk, 0) <> 1 and nvl(ddh.volume, 0) <> 0 and nvl(gtp.is_unpriced_zone, 0) = 0 then ddh.price end) as max_kc1_gtp_ppp,
          min(case when ddh.deal_type = 3 and nvl(gtp.is_fsk, 0) <> 1 and nvl(ddh.volume, 0) <> 0 and nvl(gtp.is_unpriced_zone, 0) = 0 then ddh.price end) as min_kc1_gtp_ppp,
          sum(decode(ddh.deal_type||ddh.direction, '172', ddh.volume, 0))*0.001 as volume17_2,
          sum(decode(ddh.deal_type||ddh.direction, '171', ddh.volume, 0))*0.001 as volume17_1,
          sum(decode(ddh.deal_type||ddh.direction, '182', ddh.volume, 0))*0.001 as volume18_2,
          sum(decode(ddh.deal_type||ddh.direction, '181', ddh.volume, 0))*0.001 as volume18_1,
          sum(decode(ddh.deal_type||ddh.direction, '192', ddh.volume, 0))*0.001 as volume19_2,
          sum(decode(ddh.deal_type||ddh.direction, '191', ddh.volume, 0))*0.001 as volume19_1,
          case when grouping(ddh.hour) = 0 then 1 else 2 end SHEET,
          trade_session_id tsid, oe.targ_date, oe.price_zone_code
  FROM (SELECT CASE
                  WHEN is_impex = 1
                     THEN 0
                  ELSE oes
               END oes, trader_code, trade_session_id, targ_date, price_zone_code
          FROM tsdb2.wh_trader INNER JOIN ts
               USING (trade_session_id, price_zone_code)
         WHERE trader_type = 100 AND NVL (oes, 0) != 11) oe
       INNER JOIN
       (select ddh.* from tsdb2.wh_deal_data_hour ddh, ts where ddh.trade_Session_id = ts.trade_session_id ) ddh USING (trade_session_id)
       INNER JOIN (select gtp.* from tsdb2.wh_trader gtp, ts where gtp.trade_Session_id = ts.trade_Session_id) gtp USING (trade_session_id)
 WHERE (   ddh.deal_type IN (17, 18, 19)
        OR ddh.deal_type IN (1, 3, 15) AND ddh.direction = 1
        OR ddh.deal_type = 12 AND ddh.direction = 2
       )
   AND ddh.dpg_code = oe.trader_code
   AND gtp.real_trader_id = ddh.dpg_id
group by grouping sets ((trade_session_id, oe.targ_date, oe.price_zone_code), (oe.oes,trade_session_id, oe.targ_date, oe.price_zone_code), (ddh.hour,oe.oes,trade_session_id, oe.targ_date, oe.price_zone_code), (ddh.hour,trade_session_id, oe.targ_date, oe.price_zone_code))) t1

left outer join

(select decode(grouping(oes), 0, oes, 999) oes, hour, min(min_price), max(max_price), tsid from (
    SELECT
        t.oes,
        hour,
        min(ep.price) min_price,
        max(ep.price) max_price,
        trade_session_id tsid
    FROM
        (select ep.* from tsdb2.wh_eq_prices ep, ts where ep.trade_session_id = ts.trade_session_id) ep inner join
        (
        SELECT HOUR, node_code, tt.dpg_id dpg_id, price_zone_mask, trade_session_id
       FROM (select tt.* from tsdb2.wh_eq_dout tt, ts
       where tt.trade_session_id = ts.trade_Session_id) tt
       INNER JOIN
       (SELECT   t.HOUR HOUR, t.node_code node_code, SUM (t.volume) sum_vol,
                 t.trade_session_id,
                 t.price_zone_mask
            FROM tsdb2.wh_eq_dout t INNER JOIN ts
                 ON t.price_zone_mask = ts.price_zone_code * 2
               AND t.trade_session_id = ts.trade_session_id
        GROUP BY t.HOUR, t.node_code, t.trade_session_id, t.price_zone_mask) ttt
       USING (trade_session_id, price_zone_mask, HOUR, node_code)
       WHERE ttt.sum_vol > 0
        ) ed using (trade_session_id, hour, node_code)
        inner join
        (select t.* from tsdb2.wh_trader t, ts where ts.trade_session_id = t.trade_Session_id) t using (trade_session_id)
    WHERE
        ep.price_zone_mask = ed.price_zone_mask and
        ep.price_zone_mask/2 = t.price_zone_code and
        ed.dpg_id = t.trader_id and
        node_code <> 1001199
    GROUP BY t.oes, hour, trade_session_id

        union all

        SELECT   0 oes, impx.HOUR HOUR, MIN (impx.price) min_price,
         MAX (impx.price) max_price, trade_session_id
    FROM tsdb2.wh_eq_simpexout impx INNER JOIN ts USING (trade_session_id)
   WHERE impx.dpg_type = 2 -- потребители - 1 генераторы - 2
     AND impx.volume > 0
     AND ts.price_zone_code = impx.price_zone_mask / 2
GROUP BY impx.HOUR, trade_session_id
)
group by grouping sets((oes,hour,tsid), (hour, tsid))) t2
using (tsid, oes, hour)
left outer join (select ot.* from tsdb2.wh_oes_type ot, ts where ot.trade_session_id = ts.trade_session_id) ot on (ot.oes_type = oes and ot.trade_session_id = tsid)
where oes = 999
--and hour is not null -- отключим суммирование по часам
order by oes_name desc,targ_date, oes, hour
)--where hour is null
order by oes_name desc,targ_date, hour
