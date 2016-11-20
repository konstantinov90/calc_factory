from eq_db.classes.bids import make_bids

bids = make_bids(220924901, '13.03.2016')
bid = bids[232095]
print(bid.dpg_code)
for hour in bid.hour_data.keys():
    [print(bid.hour_data[hour]['hour'], b) for b in bid.hour_data[hour]['intervals']]

# import re
#
# query = '''
# select hour, o$num rge_code, o$pmin pmin, o$pmax pmax, o$pminagg pmin_tech,
# o$p p, o$wmax wmax, o$wmin wmin, o$vgain vgain, o$vdrop vdrop, rge.parent_object_id dpg_id
# from tsdb2.wh_rastr_generator partition (&tsid) rg, tsdb2.wh_trader partition (&tsid) rge
# where rge.trader_type = 103
# and rge.trader_code = rg.o$num'''
#
#
# query = '''select trader_code dpg_code, to_number(consumer2), is_system, is_guarantee_supply_co, trader_id dpg_id,
# fed_station is_fed_station, is_disqualified, is_unpriced_zone, is_fsk, es_ref area
# from tsdb2.wh_trader partition (&tsid) gtp
# where dpg_type = 1
# and is_impex = 0
# --and trader_code = 'PSAYANAL'
# '''
#
# for match in re.finditer('(wh_(\w*)\s*partition\s*\(&tsid\))', query):
#     print(query.replace(match.group(0), match.group(1)))
#     print(match.groups())