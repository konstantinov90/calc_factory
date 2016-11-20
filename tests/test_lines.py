from eq_db.classes import lines

import mat4py
from utils import ora
from decimal import *
from operator import itemgetter
getcontext().prec = 28
getcontext().rounding = ROUND_HALF_UP


def f(x):
    return Decimal(x).quantize(Decimal('0.0000001')).quantize(Decimal('0.000001'))

con = ora.OracleConnection()

query = '''select HOUR_NUM, NODE_FROM_ID, NODE_TO_ID, PARALLEL_NUM, U_BASE, BASE_COEF, R,
                 X, G, B, KTR, LAGGING, TYPE, B_BEGIN, B_END
                 from tsdb2.wh_eq_db_lines partition (&tsid)
                 order by node_from_id, node_to_id, parallel_num, hour_num'''

dates = con.exec_script('''select trade_session_id, target_date--, note
                           from tsdb2.trade_Session
                           where trunc(target_date) >= to_date('01102016', 'ddmmyyyy')
                           and is_main = 1
                           order by target_date''')

for tsid, tdate in dates:
    ns = lines.make_lines(tsid, tdate.date())
    new_eq_db_lines = []
    for n in ns:
        new_eq_db_lines += n.get_prepared_lines_data()
    new_eq_db_lines = sorted(new_eq_db_lines, key=itemgetter(1, 2, 3, 0))

    eq_db_lines = con.exec_script(query, {'tsid': tsid})
    N = max(len(new_eq_db_lines), len(eq_db_lines))
    if len(eq_db_lines) != len(new_eq_db_lines):
        print('eq_db_lines lengths not equal!!')
    else:
        d_cntr = 0
        for i in range(0, N):
            o = list(map(f, eq_db_lines[i]))
            n = list(map(f, new_eq_db_lines[i]))
            if o != n:
                print('old', end=' # ')
                [print(ob, end='\n' if i+1 == len(o) else ' ') for i, ob in enumerate(o)]
                print('new', end=' # ')
                [print(ob, end='\n' if i+1 == len(n) else ' ') for i, ob in enumerate(n)]
                d_cntr += 1
        print('-------------- lines %i -----------------' % d_cntr)