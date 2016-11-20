from eq_db.classes import gus

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
                           where trunc(target_date) = to_date('10102016', 'ddmmyyyy')
                           and is_main = 1
                           order by target_date''')

for tsid, tdate in dates:
    ns = gus.make_gus(tsid, tdate.date())
    print(len(ns))
    for gu in ns:
        if len(gu.gu_hour_data) != 24:
            print(gu.rge_code, gu.code, len(gu.gu_hour_data))
