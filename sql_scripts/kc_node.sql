select hour, node, kc, kc_by_part, kc_total, volume_node_dpg_base,
volume_node_dpg, dpg_id
from tsdb2.wh_kc_dpg_node partition (&tsid) k
--where dpg_id = 255087
--and hour = :hour
