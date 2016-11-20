import mat4py
from utils import ora
from eq_db.distributed_bids import make_sections_data
from decimal import *
from operator import itemgetter
getcontext().prec = 28
getcontext().rounding = ROUND_HALF_UP


def f(x):
    if type(x) is str:
        return x
    return Decimal(x).quantize(Decimal('0.0000001')).quantize(Decimal('0.000001'))

con = ora.OracleConnection()

query = '''select HOUR_num, section_id, p_max_forward, p_max_backward
           from tsdb2.wh_eq_db_sections partition (&tsid) n
           where is_impex = 0
           order by hour_num, to_number(section_id)'''

dates = con.exec_script('''select trade_session_id, target_date--, note
                           from tsdb2.trade_Session
                           where trunc(target_date) >= to_date('01012016', 'ddmmyyyy')
                           and is_main = 1
                           order by target_date''')

for tsid, tdate in dates:
    sections_data = con.exec_script(query, {'tsid': tsid})
    new_sections_data = make_sections_data(tsid, tdate.date())

    new_sections_data = sorted(new_sections_data, key=itemgetter(0, 1))

    # mat4py.savemat('common.mat', {
    #     'fixedcon': new_fixedcon
    # })



    if len(sections_data) != len(new_sections_data):
        print('sections_data lengths not equal!!')
        print('new %i --- old %i' % (len(new_sections_data), len(sections_data)))
    else:
        d_cntr = 0
        for i in range(0, max(len(sections_data), len(new_sections_data))):
            o = list(map(f, sections_data[i]))
            n = list(map(f, new_sections_data[i]))
            if o != n:
                print('old', end=' # ')
                [print(ob, end='\n' if i+1 == len(o) else ' ') for i, ob in enumerate(o)]
                print('new', end=' # ')
                [print(ob, end='\n' if i+1 == len(n) else ' ') for i, ob in enumerate(n)]
                d_cntr += 1
                # break
        print('-------------- sections_data %i -----------------' % d_cntr)

