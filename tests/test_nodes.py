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

nodes_query = '''select hour_num, node_id, u_base, start_u, start_phase, price_zone, u_rated, region_id, nvl(pricezonefixed,-1)
                    from tsdb2.wh_eq_db_nodes partition (&tsid)
                    order by node_id, hour_num'''

dates = con.exec_script('''select trade_session_id, target_date--, note
                           from tsdb2.trade_Session
                           where trunc(target_date) >= to_date('01102016', 'ddmmyyyy')
                           and is_main = 1
                           order by target_date''')

for tsid, tdate in dates:
    ns = nodes.make_nodes(tsid, tdate.date())
    new_eq_db_nodes = []
    for n in ns:
        new_eq_db_nodes += n.get_eq_db_nodes_data()
    new_eq_db_nodes = sorted(new_eq_db_nodes, key=itemgetter(1, 0))

    eq_db_nodes = con.exec_script(nodes_query, {'tsid': tsid})
    N = max(len(new_eq_db_nodes), len(eq_db_nodes))
    if len(eq_db_nodes) != len(new_eq_db_nodes):
        print('eq_db_nodes lengths not equal!!')
    else:
        d_cntr = 0
        for i in range(0, N):
            o = list(map(f, eq_db_nodes[i]))
            if None in new_eq_db_nodes[i]:
                print(new_eq_db_nodes[i])
            n = list(map(f, new_eq_db_nodes[i]))
            if o != n:
                print('old', end=' # ')
                [print(ob, end='\n' if i+1 == len(o) else ' ') for i, ob in enumerate(o)]
                print('new', end=' # ')
                [print(ob, end='\n' if i+1 == len(n) else ' ') for i, ob in enumerate(n)]
                d_cntr += 1
        print('-------------- nodes %i -----------------' % d_cntr)