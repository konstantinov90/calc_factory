import mat4py
from utils import ora
from eq_db.distributed_bids import make_fixedcon_data
from decimal import *
from operator import itemgetter
getcontext().prec = 28
getcontext().rounding = ROUND_HALF_UP


def f(x):
    if type(x) is str:
        return x
    return Decimal(x).quantize(Decimal('0.0000001')).quantize(Decimal('0.000001'))

con = ora.OracleConnection()

query = '''select HOUR, NODE, GTP_CODE, VAL
            from tsdb2.wh_fixedcon partition (&tsid) n
            order by gtp_code, node, hour'''

dates = con.exec_script('''select trade_session_id, target_date--, note
                           from tsdb2.trade_Session
                           where trunc(target_date) >= to_date('01012016', 'ddmmyyyy')
                           and is_main = 1
                           order by target_date''')

for tsid, tdate in dates:
    fixedcon = con.exec_script(query, {'tsid': tsid})
    new_fixedcon = make_fixedcon_data(tsid, tdate.date())

    new_fixedcon = sorted(new_fixedcon, key=itemgetter(2, 1, 0))

    # mat4py.savemat('common.mat', {
    #     'fixedcon': new_fixedcon
    # })



    # if len(fixedcon) != len(new_fixedcon):
    #     print('fixedgen lengths not equal!!')
    print('new %i --- old %i' % (len(new_fixedcon), len(fixedcon)))
    # else:
    d_cntr = 0
    for i in range(0, max(len(fixedcon), len(new_fixedcon))):
        o = list(map(f, fixedcon[i]))
        n = list(map(f, new_fixedcon[i]))
        if o != n:
            print('old', end=' # ')
            [print(ob, end='\n' if i+1 == len(o) else ' ') for i, ob in enumerate(o)]
            print('new', end=' # ')
            [print(ob, end='\n' if i+1 == len(n) else ' ') for i, ob in enumerate(n)]
            d_cntr += 1
            # break
    print('-------------- fixedcon %i -----------------' % d_cntr)

