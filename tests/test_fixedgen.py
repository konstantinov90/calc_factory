import mat4py
from utils import ora
from eq_db.distributed_bids import make_fixedgen_data
from decimal import *
from operator import itemgetter
getcontext().prec = 28
getcontext().rounding = ROUND_HALF_UP


def f(x):
    if type(x) is str:
        return x
    return Decimal(x).quantize(Decimal('0.0000001')).quantize(Decimal('0.000001'))

con = ora.OracleConnection()

query = '''select hour, to_number(num), node, gtp_code, val
            from tsdb2.wh_fixedgen partition (&tsid)
            order by gtp_code, num, hour'''

dates = con.exec_script('''select trade_session_id, target_date--, note
                           from tsdb2.trade_Session
                           where trunc(target_date) = to_date('13032016', 'ddmmyyyy')
                           and is_main = 1
                           order by target_date''')

for tsid, tdate in dates:
    new_fixedgen = make_fixedgen_data(tsid, tdate.date())

    new_fixedgen = sorted(new_fixedgen, key=itemgetter(3, 1, 0))

    # mat4py.savemat('common.mat', {
    #     'fixedgen': new_fixedgen
    # })

    fixedgen = con.exec_script(query, {'tsid': tsid})

    if len(fixedgen) != len(new_fixedgen):
        print('fixedgen lengths not equal!!')
        print('new %i --- old %i' % (len(new_fixedgen), len(fixedgen)))
    else:
        d_cntr = 0
        for i in range(0, max(len(fixedgen), len(new_fixedgen))):
            o = list(map(f, fixedgen[i]))
            n = list(map(f, new_fixedgen[i]))
            if o != n:
                print('old', end=' # ')
                [print(ob, end='\n' if i+1 == len(o) else ' ') for i, ob in enumerate(o)]
                print('new', end=' # ')
                [print(ob, end='\n' if i+1 == len(n) else ' ') for i, ob in enumerate(n)]
                d_cntr += 1
        print('-------------- fixedgen %i -----------------' % d_cntr)

