select hour,
o_node,
o_consumer,
o_p,
O_NODEDOSE,
O_NODESTATE
from
dm_opr.model_load_ts
where scenario_fk = :scenario
