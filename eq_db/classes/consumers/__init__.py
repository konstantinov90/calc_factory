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

    for new_row in con.script_cursor(rc, tsid=DB.Partition(tsid)):
        consumer = Consumer[new_row.consumer_code]
        if not consumer:
            consumer = Consumer(new_row)
        consumer.add_consumer_hour_data(new_row)

@ts_manager
def add_consumers_vertica(scenario):
    """add Consumer instances from Vertica DB"""
    con = DB.VerticaConnection()

    for new_row in con.script_cursor(rc_v, scenario=scenario):
        consumer = Consumer[new_row.consumer_code]
        if not consumer:
            consumer = Consumer(new_row, is_new=True)

        consumer.add_consumer_hour_data(new_row)

@ts_manager
def send_consumers_to_db(ora_con):
    """save new instances to current session"""
    data = []

    for consumer in Consumer:
        if consumer.is_new:
            for _hd in consumer.hour_data:
                data.append((
                    _hd.hour, consumer.code, _hd.type, _hd.pdem, consumer.dem_rep_ready
                ))

    with ora_con.cursor() as curs:
        curs.execute('''DELETE from rastr_consumer2
                        where loading_protocol is null''')

        curs.executemany('''
            INSERT into rastr_consumer2 (hour, o$num, o$type,
            o$pdem, o$pdsready)
            values (:1, :2, :3, :4, :5)
        ''', data)
