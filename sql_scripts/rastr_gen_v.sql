select hour, g.dgu_code rge_code, sum(pmin), sum(g.pmax), sum(pmin) pmin_agg, sum(g.pmax) pmax_agg,
sum(pmin) pmin_tech, sum(g.pmax) pmax_tech, sum(pmin) pmin_heat, sum(g.pmax) pmax_heat, sum(pmin) pmin_so,
sum(g.pmax) pmax_so, sum(g.pmax) p, 0 wmax, 0 wmin, sum(g.pmax) vgain, sum(g.pmax) vdrop, node node_code
from dm_opr.model_bid_gen_ts g, dm_opr.model_gu_ts t
where g.scenario_fk = :scenario
and t.scenario_fk = g.scenario_fk
and t.dgu_code = g.dgu_code
group by hour, g.dgu_code, node
