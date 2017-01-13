select
o$ip node_from,
o$iq node_to,
o$np n_par,
o$ktr kt_re,
o$kti kt_im,
o$div div,
o$tip type,
o$na area,
hour,
o$sta state,
o$r R,
o$x X,
o$b B,
o$g G,
o$b_ip B_from,
o$b_iq B_to,
o$dp losses
from tsdb2.wh_rastr_vetv partition (&tsid)
-- where hour = 0
