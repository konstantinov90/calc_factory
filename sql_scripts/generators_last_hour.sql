select gen_id rge_code, pgenlasthour volume
from tsdb2.wh_eq_db_generators_data partition (&tsid)
