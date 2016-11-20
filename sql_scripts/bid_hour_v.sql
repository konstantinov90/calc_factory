select dpg_code bid_id, dpg_code || '_' || hour bid_hour_id,  hour, dpg_code dpg_id
from dm_opr.model_bid_gen_ts g
where g.scenario_fk = :scenario
group by dpg_code, hour
