select hour, dpg_id, to_number(rge_code) rge_code, nvl(kg,0), nvl(kg_reg,0), node,
sum(pmin) over (partition by dpg_id, hour) pmin_dpg, pmin, pminagg, pmax, dpminso, p,
sum(pminagg)  over (partition by dpg_id, hour) pminagg_dpg,
sum(pmax)  over (partition by dpg_id, hour) pmax_dpg,
nvl(kg_min,0)
from tsdb2.wh_kg_dpg_rge partition (&tsid)
--where dpg_id = :dpg_id
