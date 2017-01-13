select hour,gu_pk,DGU_CODE,GU_CODE,vg_gu_pdg,SCENARIO_FK
from dm_opr.MODEL_GEN_FIX_TS
where scenario_fk = :scenario
