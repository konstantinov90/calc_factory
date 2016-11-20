from eq_db.classes import sections

import mat4py
from utils import ora
from decimal import *
from operator import itemgetter
getcontext().prec = 28
getcontext().rounding = ROUND_HALF_UP


def f(x):
    return Decimal(x).quantize(Decimal('0.0000001')).quantize(Decimal('0.000001'))

con = ora.OracleConnection()

dates = con.exec_script('''select trade_session_id, target_date--, note
                           from tsdb2.trade_Session
                           where trunc(target_date) = to_date('01102016', 'ddmmyyyy')
                           and is_main = 1
                           order by target_date''')

for tsid, tdate in dates:
    ns = sections.make_sections(tsid, tdate.date())
    for l in ns[4010].lines:
        print(l.node_from_code, l.node_to_code, l.parallel_num)