"""Create Load instances."""
from operator import attrgetter
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import rastr_load_script as rl, rastr_load_script_v as rl_v
from .loads import Load


@ts_manager
def make_loads(tsid):
    """create Load instances"""
    con = DB.OracleConnection()
    Load.clear()

    for new_row in con.script_cursor(rl, tsid=DB.Partition(tsid)):
        load = Load[new_row.consumer_code]
        if not load:
            load = Load(new_row)
        load.add_load_hour_data(new_row)

@ts_manager
def add_loads_vertica(scenario):
    """add Load instances from vertica"""
    con = DB.VerticaConnection()

    for new_row in con.script_cursor(rl_v, scenario=scenario):
        load = Load[new_row.consumer_code]
        if not load:
            load = Load(new_row, is_new=True)
        load.add_load_hour_data(new_row)

@ts_manager
def send_loads_to_db(ora_con):
    """save new instances to current session"""
    data = []

    for load in Load:
        if load.is_new:
            for _nd in load.nodes_data:
                for _hd in _nd.hour_data:
                    data.append((
                        _hd.hour, load.consumer_code, _nd.node_code,
                        _hd.pn, _hd.node_dose
                    ))

    with ora_con.cursor() as curs:
        curs.execute('''DELETE from rastr_load
                        where loading_protocol is null''')

        curs.executemany('''
            INSERT into rastr_load (hour, o$consumer, o$node,
            o$p, o$nodedose)
            values (:1, :2, :3, :4, :5)
        ''', data)
