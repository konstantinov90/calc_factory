from eq_db.classes import loads

import mat4py
from utils import DB
from decimal import *
from operator import itemgetter
getcontext().prec = 28
getcontext().rounding = ROUND_HALF_UP


def f(x):
    return Decimal(x).quantize(Decimal('0.0000001')).quantize(Decimal('0.000001'))

con = DB.OracleConnection()

dates = con.exec_script('''select trade_session_id, target_date--, note
                           from tsdb2.trade_Session
                           where trunc(target_date) = to_date('21102016', 'ddmmyyyy')
                           and is_main = 1
                           order by target_date''')

for tsid, tdate in dates:
    ns = loads.make_loads(tsid, tdate.date())
    print(len(ns))
    for load in ns:
        for node_code, n in load.nodes.items():
            if len(n['hour_data']) != 24:
                print(load.consumer_code, node_code, len(n['hour_data']))
    print(ns[1255].consumer_code, 100748, ns[1255].nodes[100748]['hour_data'][11].pn)
