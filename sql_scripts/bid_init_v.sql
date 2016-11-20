select dpg_code, dpg_code dpg_id, dpg_code bid_id
from dm_opr.model_bid_gen_ts g
where g.scenario_fk = :scenario
group by dpg_Code
