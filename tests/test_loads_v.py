from utils import ORM
from eq_db.classes.areas import make_areas, Area
from eq_db.classes.nodes import make_nodes, Node
from eq_db.classes.loads import make_loads, Load
from eq_db.classes.lines import make_lines, Line

tsid = 220482901
scenario = 1
tdate = '31-07-2015'


ORM.recreate_all()

areas = make_areas(tsid, tdate)
# nodes = add_nodes_vertica(nodes, scenario, tdate)
nodes = make_nodes(tsid, tdate)

loads = make_loads(tsid, tdate)
