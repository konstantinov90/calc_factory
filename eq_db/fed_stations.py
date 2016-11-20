import time
from utils import ora
from utils.progress_bar import update_progress
import sql_scripts
from eq_db.classes.demands import DpgDemand
from eq_db.classes.supplies import DpgSupply
from eq_db.classes.nodes import make_eq_db_nodes
# import random


def make_dpgs(tsid, tdate=''):
# tsid = 221348901
# tdate = '01.01.1970'

    if tdate:
        print('preparing dpgs for', tdate)
    start_time = time.time()

    cs = sql_scripts.ConsumersScript()
    gs = sql_scripts.GeneratorsScript()
    bs = sql_scripts.BidsScript()
    ks = sql_scripts.KcNodeScript()
    kr = sql_scripts.KgRgeScript()
    rgs = sql_scripts.RastrGenScript()
    rs = sql_scripts.RastrConsumerScript()
    ls = sql_scripts.RastrLoadScript()

    con = ora.OracleConnection()

    DpgDemand.set_max_bid_prices(con.exec_script(sql_scripts.MaxBidPriceScript().get_query(), {'tsid': tsid}))
    DpgSupply.set_wsum_data(con.exec_script(sql_scripts.WsumgenScript().get_query(), {'tsid': tsid}))

    dpgs = []
    dpgs_index = {}
    consumers_index = {}

    @ora.process_cursor(con, cs, {'tsid': tsid})
    def process_consumers(new_row, dpg_list, dpg_list_index, consumer_list_index):
        dpg_list_index[new_row[cs['dpg_id']]] = len(dpg_list)
        consumer_list_index[new_row[cs['consumer_code']]] = len(dpg_list)
        dpg_list.append(DpgDemand(new_row))

    @ora.process_cursor(con, gs, {'tsid': tsid})
    def process_generators(new_row, dpg_list, dpg_list_index):
        dpg_list_index[new_row[gs['gtp_id']]] = len(dpg_list)
        dpg_list.append(DpgSupply(new_row))

    @ora.process_cursor(con, rgs, {'tsid': tsid})
    def process_generator_data(new_row, dpg_list, dpg_list_index):
        dpg_id = new_row[rgs['dpg_id']]
        if dpg_id in dpg_list_index.keys():
            dpg_list[dpg_list_index[dpg_id]].add_generator_data(new_row)

    @ora.process_cursor(con, bs, {'tsid': tsid})
    def process_bids(new_row, dpg_list, dpg_list_index):
        dpg_id = new_row[bs['dpg_id']]
        if dpg_id in dpg_list_index.keys():
            dpg_list[dpg_list_index[dpg_id]].add_bid_data(new_row)

    @ora.process_cursor(con, ks, {'tsid': tsid})
    def process_k_distr(new_row, dpg_list, dpg_list_index):
        dpg_id = new_row[ks['dpg_id']]
        if dpg_id in dpg_list_index.keys():
            dpg_list[dpg_list_index[dpg_id]].add_k_distr_data(new_row)
        else:
            print('!!consumer dpg %i not in list!!' % dpg_id)

    @ora.process_cursor(con, kr, {'tsid': tsid})
    def process_k_rge_distr(new_row, dpg_list, dpg_list_index):
        dpg_id = new_row[kr['dpg_id']]
        if dpg_id in dpg_list_index.keys():
            dpg_list[dpg_list_index[dpg_id]].add_k_distr_data(new_row)
        # else:
        #     print('!!supplier dpg %i not in list!!' % dpg_id)

    @ora.process_cursor(con, rs, {'tsid': tsid})
    def process_rastr_consumer(new_row, dpg_list, consumer_list_index):
        consumer_code = new_row[rs['consumer_code']]
        if consumer_code in consumer_list_index.keys():
            dpg_list[consumer_list_index[consumer_code]].add_consumer_data(new_row)

    @ora.process_cursor(con, ls, {'tsid': tsid})
    def process_rastr_load(new_row, dpg_list, consumer_list_index):
        consumer_code = new_row[ls['consumer_code']]
        if consumer_code in consumer_list_index.keys():
            dpg_list[consumer_list_index[consumer_code]].add_load_data(new_row)

    print('getting consumer DPGs')
    process_consumers(dpgs, dpgs_index, consumers_index)
    process_rastr_consumer(dpgs, consumers_index)
    process_rastr_load(dpgs, consumers_index)

    print('getting supplier DPGs')
    process_generators(dpgs, dpgs_index)

    # R = random.randint(0, len(dpgs))
    # DPGCODE = dpgs[R].code
    # DPGID = dpgs[R].id

    print('getting generator information')
    process_generator_data(dpgs, dpgs_index)

    nodes, nodes_index = make_eq_db_nodes(tsid, tdate)

    print('getting bid information')
    process_bids(dpgs, dpgs_index)
    #
    print('getting k_distr information')
    # process_k_distr(dpgs, dpgs_index)
    process_k_rge_distr(dpgs, dpgs_index)

    print("distributing consumer's bids")
    for i, d in enumerate(dpgs):
        d.finalize_data()
        d.distribute_bid()
        d.prepare_generator_data()
        if isinstance(d, DpgSupply):
            d.prepare_fixedgen_data(nodes, nodes_index)
        update_progress((i + 1) / len(dpgs))
    print('done!')
    # [print(d, i) for d, i in zip(dpgs, dpgs_index)]

    print('---------- %s seconds -----------' % (time.time() - start_time))

    # print(R, DPGCODE, dpgs[R].consumer_code, len(dpgs[dpgs_index[DPGID]].get_distributed_bid()))
    # [print(d) for d in dpgs[dpgs_index[907]].get_distributed_bid()]
    return dpgs, dpgs_index


def make_distributed_bids(tsid, tdate=''):
    dpgs, dpgs_index = make_dpgs(tsid, tdate)
    eq_db_supplies = []
    eq_db_demands = []
    for dpg in dpgs:
        if isinstance(dpg, DpgSupply):
            eq_db_supplies += dpg.get_distributed_bid()
        elif isinstance(dpg, DpgDemand):
            eq_db_demands += dpg.get_distributed_bid()
    return eq_db_supplies, eq_db_demands


def make_fixedgen_data(tsid, tdate=''):
    dpgs, dpgs_index = make_dpgs(tsid, tdate)
    fixedgen = []
    for dpg in dpgs:
        if isinstance(dpg, DpgSupply):
            fixedgen += dpg.get_fixedgen_data()
    return fixedgen


def make_generator_data(tsid, tdate=''):
    dpgs, dpgs_index = make_dpgs(tsid, tdate)
    eq_db_generators = []
    for dpg in dpgs:
        eq_db_generators += dpg.get_generator_data()
    return eq_db_generators
