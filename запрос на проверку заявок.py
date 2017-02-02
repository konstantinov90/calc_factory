'''
проверка наличия заявок у ГТП
выведутся те ГТП, у которых нет заявки
'''
dpgs = []
supplies_vol = 0
demands_vol = 0

for dpg in m.DpgDemand:
    if not dpg.is_fsk and not dpg.is_unpriced_zone and not dpg.is_fed_station:
        if not dpg.bid: # or not dpg.bid.is_new:
            dpgs.append(dpg)
        else:
            for bid_hour in dpg.bid:
                try:
                    demands_vol += bid_hour.interval_data[-1].volume
                except IndexError:
                    print('%s has no bid intervals!' % dpg.code)

for dpg in m.DpgSupply:
    if not dpg.is_blocked and not dpg.is_unpriced_zone:
        if not dpg.bid:
            dpgs.append(dpg)
        else:
            for bid_hour in dpg.bid:
                try:
                    supplies_vol += bid_hour.interval_data[-1].volume
                except IndexError:
                    print('%s has no bid intervals!' % dpg.code)

for dpg in dpgs:
    dpg

print('supplies volume %f && demands volume %f' % (supplies_vol, demands_vol))

