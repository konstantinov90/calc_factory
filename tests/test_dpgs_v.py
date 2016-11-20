from eq_db.classes.dpg_list import make_dpgs
from eq_db.vertica_corrections import add_supplies_vertica


tsid = 220482901
scenario = 1
tdate = '31-07-2015'

dpgs = make_dpgs(tsid, tdate)

dpgs = add_supplies_vertica(dpgs, scenario, tdate)
