import mat4py
from utils import ora
from utils.progress_bar import update_progress
from eq_db.distributed_bids import make_eq_db_nodes_pq
from decimal import *
from operator import itemgetter
getcontext().prec = 28
getcontext().rounding = ROUND_HALF_UP


def f(x):
    if type(x) is str:
        return x
    return Decimal(x).quantize(Decimal('0.0000001')).quantize(Decimal('0.000001'))

con = ora.OracleConnection()

# query = '''select HOUR_NUM, NODE_ID, U_BASE, P_CONS_MINUS_GEN, Q_CONS, U_MAX, U_MIN, Q_GEN, CONS, GEN
query = '''select HOUR_NUM, NODE_ID, U_BASE, P_START, U_REL, PHASE_START, Q_MAX, Q_MIN
            from tsdb2.wh_eq_db_nodes_sw partition (&tsid)
            order by node_id, hour_num'''

dates = con.exec_script('''select trade_session_id, target_date--, note
                           from tsdb2.trade_Session
                           where trunc(target_date) > to_date('06012016', 'ddmmyyyy')
                           and is_main = 1
                           order by target_date''')

for tsid, tdate in dates:
    new_eq_db_nodes_pq, new_eq_db_nodes_pv, new_eq_db_nodes_sw = make_eq_db_nodes_pq(tsid, tdate.date())

    new_eq_db_nodes_pq = sorted(new_eq_db_nodes_pq, key=itemgetter(1, 0))
    new_eq_db_nodes_pv = sorted(new_eq_db_nodes_pv, key=itemgetter(1, 0))
    new_eq_db_nodes_sw = sorted(new_eq_db_nodes_sw, key=itemgetter(1, 0))

    # mat4py.savemat('common.mat', {
    #     'fixedgen': new_fixedgen
    # })

    # eq_db_nodes_pq = con.exec_script(query, {'tsid': tsid})
    eq_db_nodes_sw = con.exec_script(query, {'tsid': tsid})

    if len(eq_db_nodes_sw) != len(new_eq_db_nodes_sw):
        print('eq_db_nodes_pq lengths not equal!!')
        print('new %i --- old %i' % (len(new_eq_db_nodes_sw), len(eq_db_nodes_sw)))
    else:
        d_cntr = 0
        n_length = max(len(eq_db_nodes_sw), len(new_eq_db_nodes_sw))
        for i in range(0, n_length):
            o = list(map(f, eq_db_nodes_sw[i]))
            n = list(map(f, new_eq_db_nodes_sw[i]))
            if o != n:
                print('old', end=' # ')
                [print(ob, end='\n' if i+1 == len(o) else ' ') for i, ob in enumerate(o)]
                print('new', end=' # ')
                [print(ob, end='\n' if i+1 == len(n) else ' ') for i, ob in enumerate(n)]
                d_cntr += 1
            update_progress((i + 1) / n_length)
        print('-------------- eq_db_nodes_sw %i -----------------' % d_cntr)

