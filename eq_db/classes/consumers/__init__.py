"""Create Consumer instances."""
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import rastr_consumer_script as rc
from sql_scripts import rastr_consumer_script_v as rc_v
from .consumers import Consumer


@ts_manager
def make_consumers(tsid):
    """create Consumer instances"""
    con = DB.OracleConnection()
    Consumer.clear()

    for new_row in con.script_cursor(rc, tsid=tsid):
        consumer = Consumer[new_row.consumer_code]
        if not consumer:
            consumer = Consumer(new_row)
        consumer.add_consumer_hour_data(new_row)

@ts_manager
def add_consumers_vertica(scenario, **kwargs):
    """add Consumer instances from Vertica DB"""
    con = DB.VerticaConnection()
    ora_con = kwargs['ora_con']
    tdate = kwargs['target_date']

    for new_row in con.script_cursor(rc_v, scenario=scenario):
        consumer = Consumer[new_row.consumer_code]
        if not consumer:
            consumer = Consumer(new_row)

        ora_con.exec_insert('''insert into rastr_consumer2 (hour, o$num, o$type,
                               o$pdem, o$pdsready)
                               values (:hour, :consumer_code, :type, :pdem, :dem_rep_ready)
                               ''', **new_row._asdict())
        consumer.add_consumer_hour_data(new_row)
