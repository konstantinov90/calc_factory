"""Create Node instances."""
from operator import itemgetter, attrgetter
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

    for new_row in con.script_cursor(ns, tsid=DB.Partition(tsid)):
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
        if not node:
            node = Node(new_row, is_new=True)
        node.add_node_hour_data(new_row)

@ts_manager
def send_nodes_to_db(ora_con):
    """save new instances to current session"""
    data = []
    attrs = 'code area_code nominal_voltage min_voltage max_voltage'.split()
    hour_attrs = '''hour type pn max_q min_q voltage fixed_voltage
                 g_shunt b_shunt qn qg pg q_shunt p_shunt b_shr'''.split()
    atg = attrgetter(*attrs)
    hour_atg = attrgetter(*hour_attrs)
    data_len = len(attrs) + len(hour_attrs) + 3
    for node in Node:
        if node.is_new:
            pz = node.price_zone * 2
            for _hd in node.hour_data:
                phase = _hd.phase / 3.1415 * 180
                state = not _hd.state
                data.append(atg(node) + hour_atg(_hd) + (state, phase, pz))

    with ora_con.cursor() as curs:
        curs.execute('''
            DELETE from rastr_node
            where loading_protocol is null
        ''')
        curs.executemany('''
        INSERT into rastr_node (o$ny, o$na, o$uhom, o$umin, o$umax,
        hour, o$tip, o$pn, o$qmax, o$qmin, o$vras, o$vzd, o$gsh, o$bsh,
        o$qn, o$qg, o$pg, o$qsh, o$psh, o$bshr, o$sta, o$delta, price_zone_mask)
        values (:{})
        '''.format(', :'.join(str(i + 1) for i in range(data_len))),
                         data)
