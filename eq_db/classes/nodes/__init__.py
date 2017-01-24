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
def add_nodes_vertica(scenario, **kwargs):
    """add Node instances from Vertica DB"""
    con = DB.VerticaConnection()
    ora_con = kwargs['ora_con']

    for new_row in con.script_cursor(ns_v, scenario=scenario):
        node = Node[new_row.node_code]

        # if Node[node_code]:
        #     raise Exception("tried to add existing node from Vertica")
        if not ora_con.exec_script('''
                    SELECT o$ny from rastr_node
                    where o$ny = :ny and hour = :hour
                ''', ny=new_row.node_code, hour=new_row.hour):
            ora_con.exec_insert('''
                INSERT into rastr_node (target_date, hour, o$ny, o$na, o$uhom, o$umin, o$umax,
                o$sta, o$tip, o$pn, o$qmax, o$qmin, o$vras, o$delta, o$vzd, o$gsh, o$bsh,
                o$qn, o$qg, o$pg, o$qsh, o$psh, o$bshr, price_zone_mask) values
                (:tdate, :hour, :node_code, :area_code, :nominal_voltage, :min_voltage,
                :max_voltage, :state, :type, :pn, :max_q, :min_q, :voltage, :phase,
                :fixed_voltage, :g_shunt, :b_shunt, :qn, :qg, :pg, :q_shunt, :p_shunt,
                :b_shr, :price_zone * 2)
                ''', tdate=kwargs.get('target_date'), **new_row._asdict())
        if not node:
            node = Node(new_row)
        # for hour in range(HOURCOUNT):
        #     row = ns_v.Tup(*(new_row[:ns_v['hour']] + (hour,) + new_row[(ns_v['hour'] + 1):]))
        node.add_node_hour_data(new_row)
