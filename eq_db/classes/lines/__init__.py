"""Create Line instances."""
from operator import attrgetter
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import lines_script as ls, lines_script_v as ls_v, lines_out_v as lo_v
from sql_scripts import lines_script_v as ls_v
from .lines import Line
from .lines import LineHourData

HOURCOUNT = 24


@ts_manager
def make_lines(tsid):
    """create Line instances"""
    con = DB.OracleConnection()

    for new_row in con.script_cursor(ls, tsid=tsid):
        node_from_code = new_row.node_from
        node_to_code = new_row.node_to
        line_par_num = new_row.n_par
        line = Line.by_key[node_from_code, node_to_code, line_par_num]
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
        line = Line.by_key[key]
        if line and key not in new_lines:
            raise Exception('Vertica contains already existing line %i -> %i: %i' % key)
        if not line:
            line = Line(new_row, is_new=True)
            new_lines.append(key)
            # for hour in range(HOURCOUNT):
            #     row = ls_v.Tup(*(new_row[:ls_v['hour']] + (hour,) + new_row[(ls_v['hour'] + 1):]))
        line.add_line_hour_data(new_row)

    for key in con.script_cursor(lo_v, scenario=scenario):
        line = Line.by_key[key]
        if line:
            line.is_turned_off = True
            for _hd in line.hour_data:
                _hd.state = False

@ts_manager
def send_lines_to_db(ora_con):
    """save new instances to current session"""
    data = []
    lines_off = []
    attrs = '''node_from_code node_to_code parallel_num kt_re kt_im
            div type area_code'''.split()
    hour_attrs = 'hour r x b g b_from b_to losses'.split()
    atg = attrgetter(*attrs)
    hour_atg = attrgetter(*hour_attrs)
    data_len = len(attrs) + len(hour_attrs) + 1
    for line in Line:
        if line.is_new:
            for _hd in line.hour_data:
                state = not _hd.state
                data.append(atg(line) + hour_atg(_hd) + (state,))
        if line.is_turned_off:
            lines_off.append(attrgetter('node_from_code', 'node_to_code', 'parallel_num')(line))

    with ora_con.cursor() as curs:
        curs.execute('''
            DELETE from rastr_vetv
            where loading_protocol is null
        ''')

        curs.executemany('''
        INSERT into rastr_vetv (o$ip, o$iq, o$np, o$ktr, o$kti, o$div, o$tip, o$na,
        hour, o$r, o$x, o$b, o$g, o$b_ip, o$b_iq, o$dp, o$sta)
        values (:{})
        '''.format(', :'.join(str(i + 1) for i in range(data_len))),
                         data)
        curs.executemany('''
        UPDATE rastr_vetv
        set o$sta = 1
        where o$ip = :1
        and o$iq = :2
        and o$np = :3
        ''', lines_off)
