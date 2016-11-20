from eq_db.classes import nodes

import mat4py
from utils import ora
from decimal import *
from operator import itemgetter
getcontext().prec = 28
getcontext().rounding = ROUND_HALF_UP


def f(x):
    return Decimal(x).quantize(Decimal('0.0000001')).quantize(Decimal('0.000001'))

con = ora.OracleConnection()

nodes_query = '''select HOUR_NUM, NODE_ID, U_BASE, G, B
                    from tsdb2.wh_eq_db_shunts partition (&tsid)
                    order by node_id, hour_num'''

dates = con.exec_script('''select trade_session_id, target_date--, note
                           from tsdb2.trade_Session
                           where trunc(target_date) >= to_date('01102016', 'ddmmyyyy')
                           and is_main = 1
                           order by target_date''')

for tsid, tdate in dates:
    ns = nodes.make_nodes(tsid, tdate.date())
    new_shunts = []
    for n in ns:
        new_shunts += n.get_shunt_data()
    new_shunts = sorted(new_shunts, key=itemgetter(1, 0))

    shunts = con.exec_script(nodes_query, {'tsid': tsid})
    N = max(len(new_shunts), len(shunts))
    if len(shunts) != len(new_shunts):
        print('eq_db_shunts lengths not equal!!')
    else:
        d_cntr = 0
        for i in range(0, N):
            o = list(map(f, shunts[i]))
            n = list(map(f, new_shunts[i]))
            if o != n:
                print('old', end=' # ')
                [print(ob, end='\n' if i+1 == len(o) else ' ') for i, ob in enumerate(o)]
                print('new', end=' # ')
                [print(ob, end='\n' if i+1 == len(n) else ' ') for i, ob in enumerate(n)]
                d_cntr += 1
        print('-------------- eq_db_shunts %i -----------------' % d_cntr)