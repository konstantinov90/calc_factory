SELECT
case when a.f$price > 30000 then 'Сообщить в отдел сопровождения!!!!' else null end res,
a.n$hour,
a.n$ns,
c.o$name,
a.f$psech,
a.f$pmax,
a.f$pmin,
a.f$price
FROM tsdb2.wh_carana$sechenout partition (&tsid) a
left join tsdb2.wh_rastr_sechen partition (&tsid) c
on a.n$hour = c.HOUR
AND a.n$ns = c.o$ns
where ABS (a.f$pmax - a.f$psech) < 0.01
OR ABS (a.f$psech - a.f$pmin) < 0.01
OR a.f$pmax < a.f$psech
OR a.f$psech < a.f$pmin
    --and a.f$price = 0

--при цене какого-либо сечения > 30000р. проверить наличие районов с отрицательными f$iprice
--и наличие районов с ценообразующими потребителями
--если потребители получили ТГ<<V_заяв, сделать Pmin оптимизируемыми в frs_parameter
ORDER BY 1, 2, 3
