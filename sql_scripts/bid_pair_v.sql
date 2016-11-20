select dpg_code || '_' || hour bid_hour_id,
row_number() over (partition by dpg_code, hour order by price) - 1 interval_number,
price,
nvl(lag(sum(pmax)) over(partition by dpg_code, hour order by price), 0) + sum(pmax) volume,
2 bid_direction, dpg_code dpg_id,
nvl(lag(sum(pmax)) over(partition by dpg_code, hour order by price), 0) + sum(pmax) volume_init
from dm_opr.model_bid_gen_ts g
where g.scenario_fk = :scenario
group by dpg_code, hour, price
order by 1, 2
