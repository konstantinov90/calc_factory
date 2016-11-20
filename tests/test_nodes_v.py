from eq_db.classes.nodes import make_nodes
from eq_db.classes.lines import make_lines
from eq_db.vertica_corrections import add_nodes_vertica
from eq_db.vertica_corrections import add_lines_vertica

tsid = 220992901
scenario = 6
tdate = '16-04-2016'

nodes = make_nodes(tsid, tdate)
nodes = add_nodes_vertica(nodes, scenario, tdate)

lines = make_lines(tsid, tdate)
lines = add_lines_vertica(lines, scenario, tdate)

lines.attach_nodes(nodes)
nodes.set_balance_nodes(lines)
