import mat4py
from utils import ora
from eq_db.distributed_bids import make_distributed_bids
from decimal import *
from operator import itemgetter
getcontext().prec = 28
getcontext().rounding = ROUND_HALF_UP


def f(x):
    return Decimal(x).quantize(Decimal('0.0000001')).quantize(Decimal('0.000001'))

con = ora.OracleConnection()

demands_query = '''select hour_num, id, interval_num, node_id, volume, price, is_accepted
                   from tsdb2.wh_eq_db_demands partition (&tsid)
                   order by node_id, hour_num, id, interval_num'''

supplies_query = '''select hour_num, node_id, p_max, p_min, cost, gen_id, interval_num, integral_constr_id, tariff, forced_sm
                    from tsdb2.wh_eq_db_supplies partition (&tsid)
                    order by node_id, hour_num, gen_id, interval_num'''

impexbids_query = '''select HOUR_NUM, DIRECTION, INTERVAL_NUM, VOLUME, PRICE, SECTION_NUMBER, IS_ACCEPTING
                     from table(tsdb2.wh_view.eq_db_impexbids(replace('&tsid','TS_','')))
                     order by section_number, direction, hour_num'''

dates = con.exec_script('''select trade_session_id, target_date--, note
                           from tsdb2.trade_Session
                           where trunc(target_date) = to_date('20062015', 'ddmmyyyy')
                           and is_main = 1
                           order by target_date''')

for tsid, tdate in dates:
    new_eq_db_supplies, new_eq_db_demands, new_eq_db_impexbids = make_distributed_bids(tsid, tdate.date())
    new_eq_db_supplies = [e for e in new_eq_db_supplies if f(e[2]) != 0]
    new_eq_db_supplies = sorted(new_eq_db_supplies, key=itemgetter(1, 0, 5, 6))
    new_eq_db_demands = sorted(new_eq_db_demands, key=itemgetter(3, 0, 1, 2))

    # mat4py.savemat('common.mat', {
    #     'eq_db_demands': new_eq_db_demands,
    #     'eq_db_supplies': new_eq_db_supplies
    # })

    eq_db_demands = []  # con.exec_script(demands_query, {'tsid': tsid})
    eq_db_supplies = []  # con.exec_script(supplies_query, {'tsid': tsid})
    eq_db_impexbids = con.exec_script(impexbids_query, {'tsid': tsid})
    # eq_db_demands = [e for e in eq_db_demands if f(e[2]) != 0]
    eq_db_supplies = [e for e in eq_db_supplies if f(e[2]) != 0]

    if len(eq_db_demands) != len(new_eq_db_demands):
        print('eq_db_demands lengths not equal!!')
    else:
        d_cntr = 0
        for i in range(0, max(len(eq_db_demands), len(new_eq_db_demands))):
            o = list(map(f, eq_db_demands[i]))
            n = list(map(f, new_eq_db_demands[i]))
            if o != n:
                print('old', end=' # ')
                [print(ob, end='\n' if i+1 == len(o) else ' ') for i, ob in enumerate(o)]
                print('new', end=' # ')
                [print(ob, end='\n' if i+1 == len(n) else ' ') for i, ob in enumerate(n)]
                d_cntr += 1
        print('-------------- demands %i -----------------' % d_cntr)

    if len(eq_db_supplies) != len(new_eq_db_supplies):
        print('eq_db_supplies lengths not equal!!')
    else:
        s_cntr = 0
        for i in range(0, max(len(eq_db_supplies), len(new_eq_db_supplies))):
            o = list(map(f, eq_db_supplies[i]))
            n = list(map(f, new_eq_db_supplies[i]))
            if o != n:
                print('old', end=' # ')
                [print(ob, end='\n' if i+1 == len(o) else ' ') for i, ob in enumerate(o)]
                print('new', end=' # ')
                [print(ob, end='\n' if i+1 == len(n) else ' ') for i, ob in enumerate(n)]
                s_cntr += 1

        print('-------------- supplies %i -----------------' % s_cntr)
    print(len(new_eq_db_impexbids))
    if len(eq_db_impexbids) != len(new_eq_db_impexbids):
        print('eq_db_impexbids lengths not equal!!')
    else:
        s_cntr = 0
        for i in range(0, max(len(eq_db_impexbids), len(new_eq_db_impexbids))):
            o = list(map(f, eq_db_impexbids[i]))
            n = list(map(f, new_eq_db_impexbids[i]))
            if o != n:
                print('old', end=' # ')
                [print(ob, end='\n' if i+1 == len(o) else ' ') for i, ob in enumerate(o)]
                print('new', end=' # ')
                [print(ob, end='\n' if i+1 == len(n) else ' ') for i, ob in enumerate(n)]
                s_cntr += 1

        print('-------------- impexbids %i -----------------' % s_cntr)
