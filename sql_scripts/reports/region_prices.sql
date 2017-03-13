

select a.target_date, x.oes_name, a.price_zone_code, a.region_name,
--a.old_volume,
a.old_price,
--a.sipr_volume,
a.sipr_price,
a.old_pz_price,
a.new_pz_price
from (
    select nvl(n.price_zone_code, o.price_zone_code) price_Zone_code,
    nvl(n.target_date, o.target_Date) target_date, nvl(n.day_type, o.day_type) day_type,
    nvl(n.region_code, o.region_code) region_code, nvl(n.region_name, o.region_name) region_name,
    o.amount old_amount, o.volume old_volume,
    CASE WHEN o.volume = 0  THEN 0 ELSE ROUND(o.amount/o.volume, 2) end old_price,
    ROUND((SUM(o.amount) OVER (PARTITION BY o.target_date, o.price_zone_code)/SUM(o.volume) OVER (PARTITION BY o.target_date, o.price_zone_code)), 2) old_pz_price,
    n.amount sipr_amount, n.volume sipr_volume,
    CASE WHEN n.volume = 0  THEN 0 ELSE ROUND(n.amount/n.volume, 2) end sipr_price,
    ROUND((SUM(n.amount) OVER (PARTITION BY n.target_date, n.price_zone_code)/SUM(n.volume) OVER (PARTITION BY n.target_date, n.price_zone_code)), 2) new_pz_price
    from (
         SELECT   t.price_zone_code, :tdate target_date,
                 TO_CHAR (:tdate, 'DY') day_type, r.region_code, r.region_name,
                 SUM(ddh.amount) AS amount,
                 SUM(ddh.volume / 1000) AS volume

            FROM tsdb2.wh_deal_data_hour partition (&tsid) ddh,
                 tsdb2.wh_trader partition (&tsid) t,
                 tsdb2.wh_region partition (&tsid) r
           WHERE ddh.direction = 1
             AND ddh.deal_type = 3
             AND ddh.dpg_id = t.trader_id
             AND t.region_code = r.region_code
             AND NVL(t.is_unpriced_zone, 0) = 0
             AND NVL(t.is_fsk, 0) = 0
           GROUP BY t.price_zone_code, ddh.target_date, r.region_name, r.region_code
    ) n full join (
         SELECT   t.price_zone_code, ddh.target_date,
                 TO_CHAR (ddh.target_date, 'DY') day_type, r.region_code, r.region_name,
                 SUM (ddh.amount) AS amount,
                 SUM (ddh.volume / 1000) AS volume,
                 sum(ddh.amount) / sum(ddh.volume)*1000 price

            FROM wh_deal_data_hour partition (&tsid_init) ddh,
                 wh_trader partition (&tsid_init) t,
                 wh_region partition (&tsid_init) r
           WHERE ddh.direction = 1
             AND ddh.deal_type = 3
             AND ddh.dpg_id = t.trader_id
             AND t.region_code = r.region_code
             AND NVL(t.is_unpriced_zone, 0) = 0
             AND NVL(t.is_fsk, 0) = 0
           GROUP BY t.price_zone_code, ddh.target_date, r.region_name, r.region_code
    ) o
    on n.price_zone_code = o.price_zone_code
    and n.region_code = o.region_code
) a left join (
    SELECT DISTINCT r.region_code, t.price_zone_code, os.oes_name
    FROM tsdb2.wh_trader partition (&tsid) t,
    tsdb2.wh_oes_type partition (&tsid) os,
    tsdb2.wh_region partition (&tsid) r
    WHERE os.oes_type = t.oes
      AND t.region_code = r.region_code
) x
on a.region_code = x.region_code
and a.price_zone_code = x.price_zone_Code
left join (
    SELECT 'ОЭС Северо-Запада' AS oes, 1 AS sort_order FROM dual
       UNION ALL
    SELECT 'ОЭС Средней Волги' AS oes, 2 AS sort_order FROM dual
       UNION ALL
    SELECT 'ОЭС Урала' AS oes, 3 AS sort_order FROM dual
        UNION ALL
    SELECT 'ОЭС Центра' AS oes, 4 AS sort_order FROM dual
        UNION ALL
    SELECT 'ОЭС Юга' AS oes, 5 AS sort_order FROM dual
        UNION ALL
    SELECT 'ОЭС Сибири' AS oes, 6 AS sort_order FROM dual
) y
on x.oes_name = y.oes
order by sort_order nulls first, price_zone_code, a.region_name
