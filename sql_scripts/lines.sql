select hour, o$ip node_from, o$iq node_to, o$np n_par, o$sta state,
o$r R, o$x X, o$b B, o$ktr kt_re, o$kti kt_im,
o$div div, o$g G, o$tip type, o$b_ip B_from, o$b_iq B_to, o$na area, o$dp losses
from tsdb2.wh_rastr_vetv partition (&tsid)
-- where hour = 0
