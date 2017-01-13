"""Create Node instances."""
from operator import itemgetter

from sql_scripts import nodes_script as ns
from sql_scripts import nodes_script_v as ns_v
from utils import DB
from utils.trade_session_manager import ts_manager
from .nodes import Node

HOURCOUNT = 24


@ts_manager
def make_nodes(tsid):
    """create Node instances"""
    con = DB.OracleConnection()
    Node.clear()

    for new_row in con.script_cursor(ns, tsid=tsid):
        node = Node[new_row.node_code]
        if not node:
            node = Node(new_row)
        node.add_node_hour_data(new_row)

@ts_manager
def add_nodes_vertica(scenario):
    """add Node instances from Vertica DB"""
    con = DB.VerticaConnection()

    for new_row in con.script_cursor(ns_v, scenario=scenario):
        node = Node[new_row.node_code]

        # if Node[node_code]:
        #     raise Exception("tried to add existing node from Vertica")
        if not node:
            node = Node(new_row)
        # for hour in range(HOURCOUNT):
        #     row = ns_v.Tup(*(new_row[:ns_v['hour']] + (hour,) + new_row[(ns_v['hour'] + 1):]))
        node.add_node_hour_data(new_row)
