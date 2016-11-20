from eq_db.classes.dgus import make_dgus
from eq_db.vertica_corrections import add_dgus_vertica

tsid = 220482901
scenario = 1
tdate = '31-07-2015'

dgus = make_dgus(tsid, tdate)
dgus = add_dgus_vertica(dgus, scenario, tdate)
