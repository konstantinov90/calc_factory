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

nodes_query = '''select hour, node, val
                    from tsdb2.wh_fixedimpex partition (&tsid)
                    order by node, hour'''

dates = con.exec_script('''select trade_session_id, target_date--, note
                           from tsdb2.trade_Session
                           where trunc(target_date) >= to_date('01012016', 'ddmmyyyy')
                           and is_main = 1
                           order by target_date''')

for tsid, tdate in dates:
    ns = nodes.make_eq_db_nodes(tsid, tdate.date())
    new_fixedimpex = []
    for n in ns:
        new_fixedimpex += n.get_impex_data()
    new_fixedimpex = sorted(new_fixedimpex, key=itemgetter(1, 0))

    fixedimpex = con.exec_script(nodes_query, {'tsid': tsid})
    N = max(len(new_fixedimpex), len(fixedimpex))
    if len(fixedimpex) != len(new_fixedimpex):
        print('fixedimpex lengths not equal!!')
        print('new %i --- old %i' % (len(new_fixedimpex), len(fixedimpex)))
    else:
        d_cntr = 0
        for i in range(0, N):
            o = list(map(f, fixedimpex[i]))
            if None in new_fixedimpex[i]:
                print(new_fixedimpex[i])
            n = list(map(f, new_fixedimpex[i]))
            if o != n:
                print('old', end=' # ')
                [print(ob, end='\n' if i+1 == len(o) else ' ') for i, ob in enumerate(o)]
                print('new', end=' # ')
                [print(ob, end='\n' if i+1 == len(n) else ' ') for i, ob in enumerate(n)]
                d_cntr += 1
        print('-------------- fixedimpex %i -----------------' % d_cntr)