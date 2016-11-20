select hour, dgu_code, gu_code, pmin, pmax, pmin pmin_t, pmax pmax_t, 1 state, pmax vgain, pmax vdrop, 0 is_sysgen, 0 repair
from dm_opr.model_bid_gen_ts g
where g.scenario_fk = :scenario
