from eq_db.classes.gus import make_gus
from eq_db.classes.dgus import make_dgus
from eq_db.vertica_corrections import add_gus_vertica
from eq_db.vertica_corrections import add_dgus_vertica


tsid = 220482901
scenario = 1
tdate = '31-07-2015'

gus = make_gus(tsid, tdate)
gus = add_gus_vertica(gus, scenario, tdate)

dgus = make_dgus(tsid, tdate)
dgus = add_dgus_vertica(dgus, scenario, tdate)

gus.set_parent_dgus(dgus)

dgus.set_to_remove()
