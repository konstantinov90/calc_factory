select hour, o$agrnum dgu_code, o$num gu_code, o$pmin pmin, o$pmax pmax,
o$pmint pmin_t, o$pmaxt pmax_t, o$state state,
o$vgain vgain, o$vdrop vdrop, o$issysgen is_sysgen, o$repair repair
from tsdb2.wh_rastr_nblock partition (&tsid)
