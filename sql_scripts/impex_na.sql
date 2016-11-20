select n$ns section_number, n$na area, n$iseur is_europe, optimized
from tsdb2.wh_impex_na partition (&tsid)