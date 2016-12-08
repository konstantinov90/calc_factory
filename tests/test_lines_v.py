from utils import ORM
from eq_db.classes.nodes import make_nodes
from eq_db.classes.lines import make_lines
# from eq_db.vertica_corrections import add_lines_vertica
# from eq_db.vertica_corrections import add_nodes_vertica

tsid = 220482901
scenario = 1
tdate = '31-07-2015'


ORM.recreate_all()

nodes = make_nodes(tsid, tdate)
# nodes = add_nodes_vertica(nodes, scenario, tdate)

lines = make_lines(tsid, tdate)
# lines = add_lines_vertica(lines, scenario, tdate)

# lines.attach_nodes(nodes)
