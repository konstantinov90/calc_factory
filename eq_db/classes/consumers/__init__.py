"""Create Consumer instances."""
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import rastr_consumer_script as rc
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
