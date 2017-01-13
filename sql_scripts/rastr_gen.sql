select hour, o$num rge_code, o$pmin pmin, o$pmax pmax, o$pminagg pmin_agg,
o$pmaxagg pmax_agg, o$dpmintech pmin_tech, o$dpmaxtech pmax_tech,
o$dpminheat pmin_heat, o$dpmaxheat pmax_heat, o$dpminso pmin_so, o$dpmaxso pmax_so,
o$p p, o$wmax wmax, o$wmin wmin, o$vgain vgain, o$vdrop vdrop,-- rge.parent_object_id dpg_id,
o$node node_code
from tsdb2.wh_rastr_generator partition (&tsid) rg--, tsdb2.wh_trader partition (&tsid) rge
--where rge.trader_type = 103
--and rge.trader_code = rg.o$num
-- where hour = 0
