from eq_db.classes.rge_groups import make_rge_groups

import mat4py
from utils import ora
from decimal import *
from operator import itemgetter
getcontext().prec = 28
getcontext().rounding = ROUND_HALF_UP


def f(x):
    return Decimal(x).quantize(Decimal('0.0000001')).quantize(Decimal('0.000001'))

con = ora.OracleConnection()

constraint_rges_query = '''select HOUR_NUM, GROUP_ID, GEN_ID
                         from tsdb2.wh_eq_db_group_constraint_rges partition (&tsid)
                         order by group_id, gen_id, hour_num'''

constraints_query = '''select HOUR_NUM, GROUP_ID, p_max, p_min
                         from tsdb2.wh_eq_db_group_constraints partition (&tsid)
                         order by group_id, hour_num'''

dates = con.exec_script('''select trade_session_id, target_date--, note
                           from tsdb2.trade_Session
                           where trunc(target_date,'mm') >= to_date('01082015', 'ddmmyyyy')
                           and is_main = 1
                           order by target_date''')

for tsid, tdate in dates:
    rge_groups = make_rge_groups(tsid, tdate.date())
    new_eq_db_constraint_rges = [(h, 0, 0) for h in range(0, 24)]
    for rge_group in rge_groups:
        new_eq_db_constraint_rges += rge_group.get_rge_data()
    new_eq_db_constraint_rges = sorted(new_eq_db_constraint_rges, key=itemgetter(1, 2, 0))

    new_eq_db_constraints = [(h, 0, 1, -1) for h in range(0, 24)]
    for rge_group in rge_groups:
        new_eq_db_constraints += rge_group.get_constraint_data()
    new_eq_db_constraints = sorted(new_eq_db_constraints, key=itemgetter(1, 0))

    eq_db_constraint_rges = con.exec_script(constraint_rges_query, {'tsid': tsid})
    N = max(len(new_eq_db_constraint_rges), len(eq_db_constraint_rges))
    if len(eq_db_constraint_rges) != len(new_eq_db_constraint_rges):
        print('eq_db_constraint_rges lengths not equal!!')
        print(len(eq_db_constraint_rges), len(new_eq_db_constraint_rges))
    else:
        d_cntr = 0
        for i in range(0, N):
            o = list(map(f, eq_db_constraint_rges[i]))
            n = list(map(f, new_eq_db_constraint_rges[i]))
            if o != n:
                print('old', end=' # ')
                [print(ob, end='\n' if i+1 == len(o) else ' ') for i, ob in enumerate(o)]
                print('new', end=' # ')
                [print(ob, end='\n' if i+1 == len(n) else ' ') for i, ob in enumerate(n)]
                d_cntr += 1
        print('-------------- eq_db_constraint_rges %i -----------------' % d_cntr)

    eq_db_constraints = con.exec_script(constraints_query, {'tsid': tsid})
    N = max(len(new_eq_db_constraints), len(eq_db_constraints))
    if len(eq_db_constraints) != len(new_eq_db_constraints):
        print('eq_db_constraints lengths not equal!!')
        print(len(eq_db_constraints), len(new_eq_db_constraints))
    else:
        d_cntr = 0
        for i in range(0, N):
            o = list(map(f, eq_db_constraints[i]))
            n = list(map(f, new_eq_db_constraints[i]))
            if o != n:
                print('old', end=' # ')
                [print(ob, end='\n' if i+1 == len(o) else ' ') for i, ob in enumerate(o)]
                print('new', end=' # ')
                [print(ob, end='\n' if i+1 == len(n) else ' ') for i, ob in enumerate(n)]
                d_cntr += 1
        print('-------------- eq_db_constraints %i -----------------' % d_cntr)
