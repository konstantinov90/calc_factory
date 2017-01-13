"""Create Line instances."""
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import lines_script as ls
from sql_scripts import lines_script_v as ls_v
from .lines import Line
from .lines import LineHourData

HOURCOUNT = 24


@ts_manager
def make_lines(tsid):
    """create Line instances"""
    con = DB.OracleConnection()
    Line.clear()

    for new_row in con.script_cursor(ls, tsid=tsid):
        node_from_code = new_row.node_from
        node_to_code = new_row.node_to
        line_par_num = new_row.n_par
        line = Line.get_line(node_from_code, node_to_code, line_par_num)
        if not line:
            line = Line(new_row)
        line.add_line_hour_data(new_row)

@ts_manager
def add_lines_vertica(scenario):
    """add Line instances from Vertica DB"""
    con = DB.VerticaConnection()

    new_lines = []

    for new_row in con.script_cursor(ls_v, scenario=scenario):
        key = (new_row.node_from, new_row.node_to, new_row.n_par)
        line = Line.get_line(*key)
        if line and key not in new_lines:
            raise Exception('Vertica contains already existing line %i -> %i: %i' % key)
        if not line:
            line = Line(new_row)
            new_lines.append(key)
            # for hour in range(HOURCOUNT):
            #     row = ls_v.Tup(*(new_row[:ls_v['hour']] + (hour,) + new_row[(ls_v['hour'] + 1):]))
        line.add_line_hour_data(new_row)
