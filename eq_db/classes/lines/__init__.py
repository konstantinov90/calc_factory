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
def add_lines_vertica(scenario, **kwargs):
    """add Line instances from Vertica DB"""
    con = DB.VerticaConnection()
    ora_con = kwargs['ora_con']

    new_lines = []

    for new_row in con.script_cursor(ls_v, scenario=scenario):
        key = (new_row.node_from, new_row.node_to, new_row.n_par)
        line = Line.get_line(*key)
        if line and key not in new_lines:
            raise Exception('Vertica contains already existing line %i -> %i: %i' % key)
        if not ora_con.exec_script('''
                    SELECT O$ip, o$iq, o$np from rastr_vetv
                    where o$ip = :ip and o$iq = :iq and o$np = :np and hour = :hour
                ''', ip=new_row.node_from, iq=new_row.node_to, np=new_row.n_par, hour=new_row.hour):
            ora_con.exec_insert('''
                INSERT into rastr_vetv (target_date, hour, o$ip, o$iq, o$np, o$sta, o$r,
                o$x, o$b, o$ktr, o$div, o$kti, o$g, o$tip, o$b_ip, o$b_iq, o$na, o$dp)
                values (:tdate, :hour, :node_from, :node_to, :n_par, :state, :r, :x, :b,
                :kt_re, :div, :kt_im, :g, :type, :b_from, :b_to, :area_code, :losses)
                ''', tdate=kwargs.get('target_date'), **new_row._asdict())
        if not line:
            line = Line(new_row)
            new_lines.append(key)
            # for hour in range(HOURCOUNT):
            #     row = ls_v.Tup(*(new_row[:ls_v['hour']] + (hour,) + new_row[(ls_v['hour'] + 1):]))
        line.add_line_hour_data(new_row)
