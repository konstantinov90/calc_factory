select dgu_code, hour, vg_gu_pdg
from dm_opr.MODEL_GEN_FIX_TS
where scenario_fk = :scenario
and is_new = 0
