import mat4py
from utils import ora
from eq_db.distributed_bids import make_generator_data
from decimal import *
from operator import itemgetter
getcontext().prec = 28
getcontext().rounding = ROUND_HALF_UP


def f(x):
    return Decimal(x).quantize(Decimal('0.0000001')).quantize(Decimal('0.000001'))

con = ora.OracleConnection()

query = '''select HOUR_NUM, GEN_ID, P_MIN, P, RAMP_UP, RAMP_DOWN, W_MAX, W_MIN
           from tsdb2.wh_eq_db_generators partition (&tsid)
           order by hour_num, gen_id'''

dates = con.exec_script('''select trade_session_id, target_date--, note
                           from tsdb2.trade_Session
                           where trunc(target_date) >= to_date('01012016', 'ddmmyyyy')
                           and is_main = 1
                           order by target_date''')
for tsid, tdate in dates:
    new_eq_db_generators = make_generator_data(tsid, tdate.date())
    new_eq_db_generators = sorted(new_eq_db_generators, key=itemgetter(0, 1))
    mat4py.savemat('common.mat', {'eq_db_generators': new_eq_db_generators})

    eq_db_generators = con.exec_script(query, {'tsid': tsid})


    if len(eq_db_generators) != len(new_eq_db_generators):
        print('eq_db_generators lengths not equal!!')
    else:
        d_cntr = 0
        for i in range(0, max(len(eq_db_generators), len(new_eq_db_generators))):
            o = list(map(f, eq_db_generators[i]))
            n = list(map(f, new_eq_db_generators[i]))
            if o != n:
                print('old', end=' # ')
                [print(ob, end='\n' if i+1 == len(o) else ' ') for i, ob in enumerate(o)]
                print('new', end=' # ')
                [print(ob, end='\n' if i+1 == len(n) else ' ') for i, ob in enumerate(n)]
                d_cntr += 1
        print('-------------- eq_db_generators %i -----------------' % d_cntr)
