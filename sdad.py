import time
from utils import ora
from utils.progress_bar import update_progress
import sql_scripts
from eq_db.classes.demands import DpgDemand
import random


start_time = time.time()

TSID = 221346901

cs = sql_scripts.ConsumersScript()
bs = sql_scripts.BidsScript()
ks = sql_scripts.KcNodeScript()

con = ora.OracleConnection()

DpgDemand.set_max_bid_price(con.exec_script(sql_scripts.MaxBidPriceScript().get_query(), {'tsid': TSID})[0][0])

dpgs = []
dpgs_index = {}


@ora.process_cursor(con, cs, {'tsid': TSID})
def process_consumers(dpg_list, dpg_list_index, new_row):
    dpg_list.append(DpgDemand(new_row))
    dpg_list_index[new_row[cs['dpg_id']]] = len(dpg_list) - 1

print('getting consumer DPGs')
process_consumers(dpgs, dpgs_index)
print('done!')


R = random.randint(0, len(dpgs))
DPGCODE = dpgs[R].code
DPGID = dpgs[R].id


@ora.process_cursor(con, bs, {'tsid': TSID, 'dpg_id': DPGID})
def process_bids(dpg_list, dpg_list_index, new_row):
    dpg_id = new_row[bs['dpg_id']]
    if dpg_id in dpg_list_index.keys():
        dpg_list[dpg_list_index[dpg_id]].add_bid_data(new_row)

print('getting bids information')
process_bids(dpgs, dpgs_index)
print('done!')


@ora.process_cursor(con, ks, {'tsid': TSID, 'dpg_id': DPGID})
def process_k_distr(dpg_list, dpg_list_index, new_row):
    dpg_id = new_row[ks['dpg_id']]
    if dpg_id in dpg_list_index.keys():
        dpg_list[dpg_list_index[dpg_id]].add_k_distr_data(new_row)

print('getting k_distr information')
process_k_distr(dpgs, dpgs_index)
print('done!')

print("distributing consumer's bids")
for i, d in enumerate(dpgs):
    d.finalize_data()
    d.distribute_bid()
    update_progress((i+1)/len(dpgs))
print('done!')
# [print(d, i) for d, i in zip(dpgs, dpgs_index)]

print('---------- %s seconds -----------' % (time.time() - start_time))

print(R, DPGCODE, dpgs[R].consumer_code, len(dpgs[dpgs_index[DPGID]].get_distributed_bid()))
[print(d) for d in dpgs[dpgs_index[DPGID]].get_distributed_bid()]


