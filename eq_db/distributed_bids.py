import time
from utils import ora
from utils.progress_bar import update_progress
import sql_scripts
from eq_db.classes.demands import DpgDemand, DpgDemandSystem, DpgDemandLoad, DpgDemandFSK
from eq_db.classes.supplies import DpgSupply
from eq_db.classes.dpg_list import DpgList
from eq_db.classes.dpg import DpgImpex
from eq_db.classes.nodes import make_nodes
from eq_db.classes.lines import make_lines
from eq_db.classes.sections import make_sections
# import random
import mat4py


def make_dpgs(tsid, tdate='', nodes=None, lines=None, sections=None):
# tsid = 221348901
# tdate = '01.01.1970'

    if tdate:
        print('preparing dpgs for', tdate)
    start_time = time.time()

    if not nodes:
        nodes = make_nodes(tsid, tdate)

    if not lines:
        lines = make_lines(tsid, tdate, nodes)

    if not sections:
        sections = make_sections(tsid, tdate, lines)

    cs = sql_scripts.ConsumersScript()
    gs = sql_scripts.GeneratorsScript()
    imp_s = sql_scripts.ImpexDpgsScript()
    bs = sql_scripts.BidsScript()
    ks = sql_scripts.KcNodeScript()
    kr = sql_scripts.KgRgeScript()
    rgs = sql_scripts.RastrGenScript()
    rs = sql_scripts.RastrConsumerScript()
    ls = sql_scripts.RastrLoadScript()
    ra = sql_scripts.RastrAreaScript()

    con = ora.OracleConnection()

    DpgDemand.set_max_bid_prices(con.exec_script(sql_scripts.MaxBidPriceScript().get_query(), {'tsid': tsid}))
    DpgDemand.add_disqualified_data(con.exec_script(sql_scripts.DisqualifiedDataScript().get_query(), {'tsid': tsid}))
    DpgSupply.set_wsum_data(con.exec_script(sql_scripts.WsumgenScript().get_query(), {'tsid': tsid}))

    # dpgs = []
    dpgs = DpgList()
    # dpgs_index = {}
    consumers_index = {}
    dpg_areas_index = {}

    @ora.process_cursor(con, cs, {'tsid': tsid})
    def process_consumers(new_row, dpg_list):
        dpg_list.add_consumer(new_row)

    @ora.process_cursor(con, gs, {'tsid': tsid})
    def process_generators(new_row, dpg_list):
        dpg_list.add_generator(new_row)

    @ora.process_cursor(con, imp_s, {'tsid': tsid})
    def process_impex_dpgs(new_row, dpg_list):
        dpg_list.add_impex(new_row)

    @ora.process_cursor(con, rgs, {'tsid': tsid})
    def process_generator_data(new_row, dpg_list):
        dpg_id = new_row[rgs['dpg_id']]
        if dpg_list[dpg_id]:
            dpg_list[dpg_id].add_generator_data(new_row)

    @ora.process_cursor(con, bs, {'tsid': tsid})
    def process_bids(new_row, dpg_list):
        dpg_id = new_row[bs['dpg_id']]
        if dpg_list[dpg_id]:
            dpg_list[dpg_id].add_bid_data(new_row)

    @ora.process_cursor(con, ks, {'tsid': tsid})
    def process_k_distr(new_row, dpg_list):
        dpg_id = new_row[ks['dpg_id']]
        if dpg_list[dpg_id]:
            dpg_list[dpg_id].add_k_distr_data(new_row)

    @ora.process_cursor(con, kr, {'tsid': tsid})
    def process_k_rge_distr(new_row, dpg_list):
        dpg_id = new_row[kr['dpg_id']]
        if dpg_list[dpg_id]:
            dpg_list[dpg_id].add_k_distr_data(new_row)

    @ora.process_cursor(con, rs, {'tsid': tsid})
    def process_rastr_consumer(new_row, dpg_list):
        consumer_code = new_row[rs['consumer_code']]
        if dpg_list.get_consumer_by_code(consumer_code):
            dpg_list.get_consumer_by_code(consumer_code).add_consumer_data(new_row)

    @ora.process_cursor(con, ls, {'tsid': tsid})
    def process_rastr_load(new_row, dpg_list):
        consumer_code = new_row[ls['consumer_code']]
        if dpg_list.get_consumer_by_code(consumer_code):
            dpg_list.get_consumer_by_code(consumer_code).add_load_data(new_row)

    @ora.process_cursor(con, ra, {'tsid': tsid})
    def process_rastr_area(new_row, dpg_list):
        area = new_row[ra['area']]
        if dpg_list.get_consumer_by_area(area):
            dpg_list.get_consumer_by_area(area).add_area_data(new_row)

    # print('getting consumer DPGs')
    # process_consumers(dpgs)
    # process_rastr_consumer(dpgs)
    # process_rastr_load(dpgs)
    # process_rastr_area(dpgs)
    # for dpg in dpgs:
    #     dpg.attach_nodes(nodes)
    #
    # print('getting supplier DPGs')
    # process_generators(dpgs)

    print('getting impex DPGs')
    process_impex_dpgs(dpgs)
    for i, d in enumerate(dpgs):
        d.attach_sections(sections)
        update_progress((i + 1) / len(dpgs))

    # R = random.randint(0, len(dpgs))
    # DPGCODE = dpgs[R].code
    # DPGID = dpgs[R].id

    # print('getting generator information')
    # process_generator_data(dpgs)



    # print('getting bid information')
    # process_bids(dpgs)
    # #
    # # print('getting k_distr information')
    # # process_k_distr(dpgs)
    # # process_k_rge_distr(dpgs)
    #
    # print("distributing consumer's bids")
    # for i, d in enumerate(dpgs):
    #     d.finalize_data()
    #     d.distribute_bid()
    #     d.prepare_generator_data()
    #     d.prepare_fixedgen_data(nodes)
    #     d.attach_to_fed_station(dpgs)
    #     update_progress((i + 1) / len(dpgs))
    #
    # for i, d in enumerate(dpgs):
    #     d.prepare_fixedcon_data()
    #     update_progress((i + 1) / len(dpgs))
    # print('done!')
    # [print(d, i) for d, i in zip(dpgs, dpgs_index)]

    print('---------- %s seconds -----------' % (time.time() - start_time))

    # print(R, DPGCODE, dpgs[R].consumer_code, len(dpgs[dpgs_index[DPGID]].get_distributed_bid()))
    # [print(d) for d in dpgs[dpgs_index[907]].get_distributed_bid()]
    return dpgs, sections


def make_distributed_bids(tsid, tdate=''):
    dpgs = make_dpgs(tsid, tdate)
    eq_db_supplies = []
    eq_db_demands = []
    eq_db_impexbids = []
    for dpg in dpgs:
        if isinstance(dpg, DpgSupply):
            eq_db_supplies += dpg.get_distributed_bid()
        elif isinstance(dpg, DpgDemand):
            eq_db_demands += dpg.get_distributed_bid()
        elif isinstance(dpg, DpgImpex):
            eq_db_impexbids += dpg.get_distributed_bid()
    return eq_db_supplies, eq_db_demands, eq_db_impexbids


def make_fixedgen_data(tsid, tdate=''):
    dpgs = make_dpgs(tsid, tdate)
    fixedgen = []
    for dpg in dpgs:
        if isinstance(dpg, DpgSupply):
            fixedgen += dpg.get_fixedgen_data()
    return fixedgen


def make_generator_data(tsid, tdate=''):
    dpgs = make_dpgs(tsid, tdate)
    eq_db_generators = []
    for dpg in dpgs:
        if isinstance(dpg, DpgSupply):
            eq_db_generators += dpg.get_prepared_generator_data()
    return eq_db_generators


def make_fixedcon_data(tsid, tdate=''):
    dpgs = make_dpgs(tsid, tdate)
    fixedcon = []
    for dpg in dpgs:
        if isinstance(dpg, DpgDemand):
            fixedcon += dpg.get_fixedcon_data()
    return fixedcon


def make_eq_db_nodes_pq(tsid, tdate=''):
    nodes = make_nodes(tsid, tdate)
    dpgs = make_dpgs(tsid, tdate, nodes)

    eq_db_nodes_pq = []
    eq_db_nodes_pq_index = {}
    ns_pq = {'p_cons_minus_gen': 3, 'cons': 8, 'gen': 9}

    eq_db_nodes_pv = []
    eq_db_nodes_pv_index = {}
    ns_pv = {'p_gen': 3, 'cons': 12, 'gen': 13}

    eq_db_nodes_sw = []

    fis = {'hour': 0, 'node_code': 1, 'value': 2}
    for i, node in enumerate(nodes):
        node_pq_index = {}
        node_pv_index = {}
        fixedimpex = node.get_impex_data()

        for hour, node_hour in enumerate(node.hour_data):
            if not node_hour.is_node_on():
                continue
            fixedimpex_hour = [f for f in fixedimpex if f[fis['hour']] == hour]
            if len(fixedimpex_hour) > 1:
                raise Exception('wrong fixedimpex count for node %i' % node.node_code)
            if fixedimpex_hour:
                impex_value = fixedimpex_hour[0][fis['value']]
            else:
                impex_value = 0

            if node_hour.type == 1:
                node_pq_index[hour] = len(eq_db_nodes_pq)
                eq_db_nodes_pq.append([
                    hour, node.node_code, node.voltage_class, impex_value, node_hour.qn, 1.4 * node.voltage_class,
                    0.75 * node.voltage_class, node_hour.qg, max(impex_value, 0), -1 * min(impex_value, 0)
                ])
            elif node_hour.type > 1:
                node_pv_index[hour] = len(eq_db_nodes_pv)
                eq_db_nodes_pv.append([
                    hour, node.node_code, node.voltage_class, impex_value, max(node_hour.max_q, -10000),
                    min(node_hour.min_q, 10000), 1.4 * node.voltage_class, 0.75 * node.voltage_class,
                    node_hour.qn, node_hour.qg, node_hour.type, node.fixed_voltage, -1 * min(impex_value, 0),
                    max(impex_value, 0)
                ])
            elif node_hour.is_balance_node():
                relative_voltage = (node.fixed_voltage if node.fixed_voltage else node.nominal_voltage)\
                                                                                 / node.voltage_class
                eq_db_nodes_sw.append((
                    hour, node.node_code, node.voltage_class, -node_hour.pn, relative_voltage, 0,
                    max(node_hour.max_q, -10000), min(node_hour.min_q, 10000)
                ))

        if node_pq_index:
            eq_db_nodes_pq_index[node.node_code] = node_pq_index
        if node_pv_index:
            eq_db_nodes_pv_index[node.node_code] = node_pv_index

    # fixedgen = []
    fgs = {'hour': 0, 'node_code': 2, 'value': 4}
    for i, dpg in enumerate(dpgs):
        if isinstance(dpg, DpgSupply):
            for fixedgen in dpg.get_fixedgen_data():
                node_code = fixedgen[fgs['node_code']]
                hour = fixedgen[fgs['hour']]
                if node_code in eq_db_nodes_pq_index.keys():
                    ni = eq_db_nodes_pq_index[node_code]
                    if hour not in ni.keys():
                        continue
                    eq_db_nodes_pq[ni[hour]][ns_pq['p_cons_minus_gen']] += fixedgen[fgs['value']]
                    eq_db_nodes_pq[ni[hour]][ns_pq['gen']] += -fixedgen[fgs['value']]

                elif node_code in eq_db_nodes_pv_index.keys():
                    ni = eq_db_nodes_pv_index[node_code]
                    if hour not in ni.keys():
                        continue
                    eq_db_nodes_pv[ni[hour]][ns_pv['p_gen']] += fixedgen[fgs['value']]
                    eq_db_nodes_pv[ni[hour]][ns_pv['gen']] += fixedgen[fgs['value']]

    # fixedcon = []
    fcs = {'hour': 0, 'node_code': 1, 'value': 3}
    for i, dpg in enumerate(dpgs):
        if isinstance(dpg, DpgDemand):
            for fixedcon in dpg.get_fixedcon_data():
                node_code = fixedcon[fcs['node_code']]
                hour = fixedcon[fcs['hour']]
                if node_code in eq_db_nodes_pq_index.keys():
                    ni = eq_db_nodes_pq_index[node_code]
                    if hour not in ni.keys():
                        continue
                    eq_db_nodes_pq[ni[hour]][ns_pq['p_cons_minus_gen']] += fixedcon[fcs['value']]
                    eq_db_nodes_pq[ni[hour]][ns_pq['cons']] += fixedcon[fcs['value']]

                elif node_code in eq_db_nodes_pv_index.keys():
                    ni = eq_db_nodes_pv_index[node_code]
                    if hour not in ni.keys():
                        continue
                    eq_db_nodes_pv[ni[hour]][ns_pv['p_gen']] += fixedcon[fcs['value']]
                    eq_db_nodes_pv[ni[hour]][ns_pv['cons']] += -fixedcon[fcs['value']]

    # fixedimpex = []
    # fis = {'hour': 0, 'node_code': 1, 'value': 2}
    # for n in nodes:
    #     fixedimpex += n.get_impex_data()

    return eq_db_nodes_pq, eq_db_nodes_pv, eq_db_nodes_sw


def make_sections_impex_data(tsid, tdate=''):
    dpgs, sections = make_dpgs(tsid, tdate)
    sections_impex = []
    for s in sections:
        sections_impex += s.get_section_impex_data()
    return sections_impex


def make_sections_lines_impex_data(tsid, tdate=''):
    dpgs, sections = make_dpgs(tsid, tdate)
    sections_lines_impex_data = []
    for s in sections:
        sections_lines_impex_data += s.get_section_lines_impex_data()
    return sections_lines_impex_data


def make_sections_data(tsid, tdate=''):
    dpgs, sections = make_dpgs(tsid, tdate)
    sections_data = []
    for s in sections:
        sections_data += s.get_section_data()
    return sections_data


def make_sections_lines_data(tsid, tdate=''):
    dpgs, sections = make_dpgs(tsid, tdate)
    sections_lines_data = []
    for s in sections:
        sections_lines_data += s.get_section_lines_data()
    return sections_lines_data










