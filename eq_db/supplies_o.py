import time
from utils import ora
from sql_scripts import *
from operator import itemgetter
import mat4py
from decimal import *
getcontext().prec = 28
getcontext().rounding = ROUND_HALF_UP


def check_number(number):
    # return number > 0
    return Decimal(number).quantize(Decimal('0.0000001')).quantize(Decimal('0.000001')) > 0


def make_eq_db_supplies(tsid, tdate=''):
    if tdate:
        print('making eq_db_supplies for', tdate)

    start_time = time.time()

    PMINTECHPRICE = 0
    PMINTECHINTERVAL = -20
    PMINPRICE = 0.01
    PMININTERVAL = -18
    TARIFF = 9999
    FORCEDSMOOTH = 0
    PRICEACC = 0.8
    GESSTATIONTYPE = 2
    GESINTERVAL = 0

    con = ora.OracleConnection()

    gs = GeneratorsScript()
    bs = BidsScript()
    ks = KgRgeScript()
    rgs = RastrGenScript()
    ws = WsumgenScript()

    gtps = con.exec_script(gs.get_query(), {'tsid': tsid})
    # rges = con.exec_script(rgs.get_query())
    wsumgen = con.exec_script(ws.get_query(), {'tsid': tsid})


    def index_wsumgen(row):
        return row[ws['rge_code']]
    ws_index = list(map(index_wsumgen, wsumgen))

    # def index_rges(rges_row):
    #     return rges_row[rgs['hour']], rges_row[rgs['rge_code']]
    #
    # rges_index = list(map(index_rges, rges))


    # cntr = 0
    data = []
    for g in gtps:
        # print(g[gs['dpg_code']])
        dpg_bids = []
        if g[gs['station_type']] != GESSTATIONTYPE:
            dpg_bids = sorted(
                con.exec_script(bs.get_query(), {'dpg_code': g[gs['dpg_code']], 'tsid': tsid}),
                key=itemgetter(bs['hour'], bs['interval_number']))
        # dpg_bids = sorted(
        #     [bid for bid in bids if bid[bs['dpg_code']] == d[gs['dpg_code']]]
        #     , key=itemgetter(bs['hour'], bs['interval_number']))
        rge_kg = sorted(
             con.exec_script(ks.get_query(), {'dpg_id': g[gs['gtp_id']], 'tsid': tsid}),
             key=itemgetter(ks['hour']))

        for k in rge_kg:
            # rge_data = rges[rges_index.index((k[ks['hour']], k[ks['rge_code']]))]
            pmax = k[ks['pmax']]

            # if rge_data:
            #     prev_volume = rge_data[rgs['pmin']]
            #     pmax = rge_data[rgs['pmax']]
            #     if rge_data[rgs['pmin_tech']]:
            #         cur_row = (k[ks['hour']], k[ks['node']], rge_data[rgs['pmin_tech']], rge_data[rgs['pmin_tech']],
            #                    PMINTECHPRICE, k[ks['rge_code']], PMINTECHINTERVAL, 0, TARIFF, FORCEDSMOOTH)
            #         data.append(cur_row)
            #     volume = rge_data[rgs['pmin']] - rge_data[rgs['pmin_tech']]
            #     if volume:
            #         cur_row = (k[ks['hour']], k[ks['node']], volume, volume,
            #                    PMINPRICE, k[ks['rge_code']], PMININTERVAL, 0, TARIFF, FORCEDSMOOTH)
            #         data.append(cur_row)

            prev_volume = k[ks['pmin']]
            if k[ks['rge_code']] in ws_index:
                volume = k[ks['pmin']]
            else:
                volume = max(k[ks['pminagg']], k[ks['dpminso']],
                             k[ks['pmin']] if g[gs['station_type']] == GESSTATIONTYPE else 0)
            if check_number(volume):
                cur_row = (k[ks['hour']], k[ks['node']], volume, volume,
                           PMINTECHPRICE, k[ks['rge_code']], PMINTECHINTERVAL, 0, TARIFF, FORCEDSMOOTH)
                data.append(cur_row)

            volume = k[ks['pmin']] - volume
            if check_number(volume):
                cur_row = (k[ks['hour']], k[ks['node']], volume, volume,
                           PMINPRICE, k[ks['rge_code']], PMININTERVAL, 0, TARIFF, FORCEDSMOOTH)
                data.append(cur_row)

            if k[ks['rge_code']] in ws_index:
                if [w for w in wsumgen if w[ws['rge_code']] == k[ks['rge_code']]][0][ws['hour_start']]\
                   <= k[ks['hour']] <= [w for w in wsumgen if w[ws['rge_code']] == k[ks['rge_code']]][0][ws['hour_end']]:
                    w = wsumgen[ws_index.index(k[ks['rge_code']])]
                    integral_id = w[ws['integral_id']] if w[ws['integral_id']] else 0
                    volume = pmax - prev_volume
                    if check_number(volume):
                        cur_row = (k[ks['hour']], k[ks['node']], volume, 0, w[ws['price']],
                                   k[ks['rge_code']], GESINTERVAL, w[ws['integral_id']], TARIFF, FORCEDSMOOTH)

                        data.append(cur_row)
            elif g[gs['station_type']] == GESSTATIONTYPE:
                volume = min(k[ks['p']] - prev_volume, pmax - prev_volume)
                if check_number(volume):
                    min_volume = volume
                    price = PMINPRICE
                    cur_row = (k[ks['hour']], k[ks['node']], volume, min_volume,
                               price, k[ks['rge_code']], GESINTERVAL, 0, TARIFF, FORCEDSMOOTH)

                    data.append(cur_row)
            else:
                for bid in dpg_bids:
                    if bid[bs['hour']] == k[ks['hour']]:
                        if k[ks['pmax_dpg']] > k[ks['pmin_dpg']]:
                            if min(bid[bs['volume']], k[ks['pmax_dpg']]) <= k[ks['pmin_dpg']]:
                                k_distr = k[ks['kg_min']]
                            else:
                                k_distr = k[ks['kg_reg']]
                        else:
                            k_distr = k[ks['kg']]

                        bid_rge = (bid[bs['volume']] - k[ks['pmin_dpg']]) * k_distr + k[ks['pmin']]
                        volume = min(bid_rge - prev_volume, pmax - prev_volume)

                        if check_number(volume):
                            prev_volume = bid_rge
                            min_volume = volume if bid[bs['interval_number']] < 0 else 0
                            price_acc = PMINPRICE if g[gs['is_pintsch_gas']] else PRICEACC
                            price = bid[bs['price']] if bid[bs['price']] > 0 else price_acc
                            cur_row = (bid[bs['hour']], k[ks['node']], volume, min_volume,
                                       price, k[ks['rge_code']], bid[bs['interval_number']], 0, TARIFF, FORCEDSMOOTH)

                            data.append(cur_row)

    data = sorted(data, key=itemgetter(1, 0, 5, 6))
    mat4py.savemat('common.mat', {'eq_db_supplies': data})

    print('---------- %s seconds -----------' % (time.time() - start_time))

