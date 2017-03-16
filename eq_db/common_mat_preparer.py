"""module for preparing common.mat"""
from operator import attrgetter, itemgetter
import mat4py
import constants as C
from utils import DB
from utils.csv_comparator import csv_comparator
from . import model as m

def hourize(input_data, zero_size=mat4py.ZeroSize(0)):
    """break data hour-wise"""
    ret_data = []
    for hour in range(C.HOURCOUNT):
        data = [d[1:] for d in input_data if d[0] == hour]
        ret_data.append([
            {'InputData': [data] if len(data) > 1 else data if data else zero_size}
        ])
    return ret_data

def hourize_group_constraints(group_data, constraint_data):
    """break group constraint data hour-wise"""
    ret_data = []
    for hour in range(C.HOURCOUNT):
        data = [d[1:] for d in group_data if d[0] == hour]
        rges = []
        for inp_d in data:
            rges_data = [(d[2],) for d in constraint_data if d[:2] == (hour, inp_d[0])]
            rges.append([{'InputData': [rges_data]}])
        ret_data.append([{
            'InputData': [data],
            'Rges': rges
            }])
    return [ret_data]

def transpose(input_data):
    """transpose cell array"""
    def aux_gen(data):
        """aux generator"""
        length = len(data[0])
        ret = tuple()
        for idx in range(length):
            for row in data:
                ret += (row[idx],)
                if len(ret) == length:
                    yield ret
                    ret = tuple()
    return [list(aux_gen(input_data))]

def prepare_data_for_common():
    """fill common_preparer properties"""

    cpr = common_preparer
    cpr.clear()

    ia = [tuple(i if i else 0 for i in impex_area.get_impex_area_data())
          for impex_area in m.ImpexArea
          if impex_area.get_impex_area_data()[0]]

    cpr.ia = ia

    pzd = []
    pz_dr = []
    for price_zone in m.PriceZone:
        pzd += price_zone.get_consumption()
        pz_dr.append(price_zone.get_settings())

    cpr.pzd = pzd
    cpr.pz_dr = pz_dr

    fd = [wsum.get_fuel_data() for wsum in m.Wsumgen]

    cpr.fd = fd

    eg = []
    for dgu in m.Dgu:
        eg += dgu.get_prepared_generator_data()

    cpr.eg = eg

    lh = [dgu.get_last_hour_data() for dgu in m.Dgu if dgu.get_last_hour_data()]

    cpr.lh = lh

    cs = []
    crs = []
    for dgu_group in m.DguGroup:
        cs += dgu_group.get_constraint_data()
        crs += dgu_group.get_dgu_data()

    cpr.cs = cs
    cpr.crs = crs

    es = []
    for dpg in m.DpgSupply:
        es += dpg.get_distributed_bid()

    cpr.es = es

    ed = []
    for dpg in m.DpgDemand:
        ed += dpg.get_distributed_bid()

    cpr.ed = ed

    eimp = []
    for dpg in m.DpgImpex:
        eimp += dpg.get_distributed_bid()

    cpr.eimp = eimp

    eq = []
    pq = []
    pv = []
    sw = []
    sh = []
    for node in sorted(m.Node, key=attrgetter('code')):
        eq += node.get_eq_db_nodes_data()
        pq += node.get_pq_data()
        pv += node.get_pv_data()
        sw += node.get_sw_data()
        sh += node.get_shunt_data()

    cpr.eq = eq
    cpr.pq = pq
    cpr.pv = pv
    cpr.sw = sw
    cpr.sh = sh

    el = []
    for line in m.Line:
        el += line.get_eq_db_lines_data()

    cpr.el = el

    s = []
    sl = []
    si = []
    sli = []
    for section in m.Section:
        s += section.get_section_data()
        sl += section.get_section_lines_data()
        si += section.get_section_impex_data()
        sli += section.get_section_lines_impex_data()

    cpr.s = s
    cpr.sl = sl
    cpr.si = si
    cpr.sli = sli

    stngs = [setting.get_settings_data() for setting in m.Setting]

    cpr.stngs = stngs

    dr = []
    for dpg in m.DpgDemand:
        dr += dpg.get_demand_response_data()

    cpr.dr = dr

    ps = [peak_so.get_peak_so_data() for peak_so in m.PeakSO]

    cpr.ps = ps

def save_mat_file(matfile_name):
    """hourize and save data to mat-file"""
    cpr = common_preparer
    data_to_load = {
        'Nodes': hourize(cpr.eq),
        'NodesPQ': hourize(cpr.pq),
        'NodesPV': hourize(cpr.pv),
        'NodesSW': hourize(cpr.sw),
        'Shunts': hourize(cpr.sh),
        'Lines': hourize(cpr.el),
        'GroupConstraints': hourize_group_constraints(cpr.cs, cpr.crs),
        'GroupConstraintsRges': hourize(cpr.crs),
        'Sections': hourize(cpr.s),
        'SectionLines': hourize(cpr.sl),
        'SectionsImpex': hourize(cpr.si),
        'SectionLinesImpex': hourize(cpr.sli),
        'SectionRegions': {'InputData': [cpr.ia]},
        'Demands': hourize(cpr.ed),
        'Supplies': hourize(cpr.es),
        'ImpexBids': hourize(cpr.eimp, mat4py.ZeroSize(5)),
        'Generators': hourize(cpr.eg),
        'PriceZoneDemands': hourize(cpr.pzd),
        'Fuel': {'InputData': [cpr.fd]},
        'GeneratorsDataLastHour': {'InputData': [cpr.lh]},
        'HourNumbers': [{i} for i in range(C.HOURCOUNT)],
        'Settings': {'InputData': transpose(cpr.stngs)},
        'DemandResponse': {'InputData': [cpr.dr] if cpr.dr else mat4py.ZeroSize(3)},
        'PeakSo': {'InputData': [cpr.ps]},
        'PriceZoneSettings': {'InputData': [cpr.pz_dr]}
    }

    mat4py.savemat(matfile_name, {
        'HourData': data_to_load, 'Fuel': data_to_load['Fuel']})

def compare_data(tsid_init):
    """compare prepared data and initial data"""
    cpr = common_preparer
    tsid = DB.Partition(tsid_init)

    cpr.ia.sort(key=itemgetter(2, 0, 1))
    cpr.pzd.sort(key=itemgetter(0, 1))
    cpr.fd.sort(key=itemgetter(0, 1))
    cpr.eg.sort(key=itemgetter(1, 0))
    cpr.lh.sort(key=itemgetter(0))
    cpr.cs.sort(key=itemgetter(1, 0))
    cpr.crs.sort(key=itemgetter(1, 0, 2))
    cpr.es.sort(key=itemgetter(5, 0, 6))
    cpr.ed.sort(key=itemgetter(2, 4, 0, 3))
    cpr.eimp.sort(key=itemgetter(1, 0, 3))
    cpr.el.sort(key=itemgetter(1, 2, 3, 0))
    cpr.s.sort(key=itemgetter(1, 0))
    cpr.sl.sort(key=itemgetter(5, 2, 3, 1, 0))
    cpr.si.sort(key=itemgetter(1, 0))
    cpr.sli.sort(key=itemgetter(5, 2, 3, 1, 0))

    csv_comparator(cpr.ia, '''
        SELECT section_id, nvl(region_id, 0), price_zone
        from tsdb2.wh_eq_db_section_regions partition (&tsid)
        where section_id != 0
        order by price_zone, section_id, region_id
    ''', 'eq_db_section_regions_data.csv', 2, 0, 1, tsid=tsid)

    csv_comparator(cpr.pzd, '''
        SELECT a.hour_num, a.price_zone, a.p_cons_sum_value
        FROM tsdb2.wh_eq_db_price_zone_sum_values partition (&tsid) a
        order by hour_num, price_zone
    ''', 'eq_db_price_zone_sum_values_data.csv', 1, 0, tsid=tsid)

    csv_comparator(cpr.fd, '''
        SELECT id, gen_id, p_min, p_max, hour_start, hour_end
        FROM tsdb2.wh_eq_db_fuel partition (&tsid) a
        order by id, gen_id
    ''', 'eq_db_fuel_data.csv', 0, 1, tsid=tsid)

    csv_comparator(cpr.eg, '''
        SELECT hour_num, gen_id, p_min, p, ramp_up, ramp_down,
        nvl(pnpr,0), nvl(pvpr,0), nvl(station_type,0), nvl(node,0)
        from tsdb2.wh_eq_db_generators partition (&tsid)
        order by gen_id, hour_num
    ''', 'eq_db_generators_data.csv', 1, 0, tsid=tsid)

    csv_comparator(cpr.lh, '''
        SELECT gen_id, pgenlasthour
        from tsdb2.wh_eq_db_generators_data partition (&tsid)
        order by gen_id
    ''', 'eq_db_generators_last_hour_data.csv', 0, tsid=tsid)

    csv_comparator(cpr.cs, '''
        SELECT hour_num, group_id, p_max, p_min
        from tsdb2.wh_eq_db_group_constraints partition (&tsid)
        where group_id <> 0
        order by group_id, hour_num
    ''', 'eq_db_constraints_data.csv', 1, 0, tsid=tsid)

    csv_comparator(cpr.crs, '''
        SELECT hour_num, group_id, gen_id
        from tsdb2.wh_eq_db_group_constraint_rges partition (&tsid)
        where group_id <> 0
        order by group_id, hour_num, gen_id
    ''', 'eq_db_constraint_rge_data.csv', 1, 0, 2, tsid=tsid)

    csv_comparator(cpr.es, '''
        SELECT hour_num, node_id, p_max, p_min, cost, gen_id, interval_num,
        integral_constr_id, nvl(tariff,9999), forced_sm
        from tsdb2.wh_eq_db_supplies partition (&tsid)
        where p_max > 1e-10

        union all

        select n$hour, n$node, f$volume, f$volume, 0, n$objectid,
        -20, 0, 9999, 0
        from tsdb2.wh_fixedgen_rge partition (&tsid)
        order by gen_id, hour_num, interval_num
    ''', 'eq_db_supplies_data.csv', 5, 0, 6, tsid=tsid)

    csv_comparator([e[:1]+e[2:] for e in cpr.ed], '''
        SELECT hour_num, id, interval_num, node_id, volume, price, nvl(is_accepted,0)
        from tsdb2.wh_eq_db_demands partition (&tsid)

        union all

        select n$hour, n$objectid,
        case when trader_code in ('PCHITAZN','PAMUREZN') then -55
             when fed_Station = 1 then -52
             when is_gaes = 1 then -32 end interval_num,
        n$node, f$volume, hr.bid_max_price*1e6, 1
        from tsdb2.wh_fixedcon_consumer partition (&tsid) f,
        tsdb2.wh_trader partition (&tsid) t,
        tsdb2.wh_hour partition (&tsid) hr
        where t.consumer2 = f.n$objectid
        and hr.hour = f.n$hour

        order by id, node_id, hour_num, interval_num
    ''', 'eq_db_demands_data.csv', 1, 3, 0, 2, tsid=tsid)

    csv_comparator(cpr.eimp, '''
        SELECT hour_num, section_number, direction, interval_num, volume, price, is_accepting
        FROM tsdb2.wh_eq_db_impexbids partition (&tsid) a
        order by section_number, hour_num, interval_num
    ''', 'eq_db_impexbids_data.csv', 1, 0, 3, tsid=tsid)

    csv_comparator(cpr.eq, '''
        SELECT hour_num, node_id, u_base, start_u, start_phase, u_rated,
        region_id, price_zone, nvl(pricezonefixed, -1), nvl(is_price_zone, 0)
        FROM tsdb2.wh_eq_db_nodes PARTITION (&tsid)
        ORDER BY node_id, hour_num
    ''', 'eq_db_nodes_data.csv', 1, 0, tsid=tsid)

    csv_comparator(cpr.pq, '''
        SELECT hour_num, node_id, u_base, p_cons_minus_gen,
        q_cons, q_gen, u_max, u_min, cons, gen
        from tsdb2.wh_eq_db_nodes_pq partition (&tsid)
        order by node_id, hour_num
    ''', 'eq_db_nodes_pq_data.csv', 1, 0, tsid=tsid)

    csv_comparator(cpr.pv, '''
        SELECT hour_num, node_id, u_base, p_gen, q_cons,
        q_gen, type, U, q_max, q_min, U_max, u_min, cons, gen
        FROM tsdb2.wh_eq_db_nodes_pv PARTITION (&tsid)
        ORDER BY node_id, hour_num
    ''', 'eq_db_nodes_pv_data.csv', 1, 0, tsid=tsid)

    csv_comparator(cpr.sw, '''
        SELECT hour_num, node_id, U_base, u_rel, phase_start, P_start,
        q_max, q_min, nvl(is_main_for_dr,0)
        FROM tsdb2.wh_eq_db_nodes_sw PARTITION (&tsid)
        ORDER BY node_id, hour_num
    ''', 'eq_db_nodes_sw_data.csv', 1, 0, tsid=tsid)

    csv_comparator(cpr.sh, '''
        SELECT hour_num, node_id, U_base, g, b
        FROM tsdb2.wh_eq_db_shunts PARTITION (&tsid)
        ORDER BY node_id, hour_num
    ''', 'eq_db_shunts_data.csv', 1, 0, tsid=tsid)

    csv_comparator(cpr.el, '''
        SELECT hour_num, node_from_id, node_tO_id, parallel_num, type,
        u_base, base_coef, r,x,g,b,ktr, lagging, b_begin, b_end
        FROM tsdb2.wh_eq_db_lines PARTITION (&tsid)
        ORDER BY node_from_id, node_tO_id, parallel_num, hour_num
    ''', 'eq_db_lines_data.csv', 1, 2, 3, 0, tsid=tsid)

    csv_comparator(cpr.s, '''
        SELECT hour_num, section_id, p_max_forward, p_max_backward
        from tsdb2.wh_eq_db_sections partition (&tsid)
        where is_impex = 0
        order by section_id, hour_num
    ''', 'eq_db_sections_data.csv', 1, 0, tsid=tsid)

    csv_comparator(cpr.sl, '''
        SELECT sl.hour_num, sl.parallel_num, node_from_id, node_to_id,
        div_coef, sl.section_id
        from tsdb2.wh_eq_db_section_lines partition (&tsid) sl,
        tsdb2.wh_eq_db_sections partition (&tsid) s
        where s.is_impex = 0
        and s.hour_num = sl.hour_num
        and s.section_id = sl.section_id
        order by sl.section_id, node_from_id, node_to_id, sl.parallel_num, sl.hour_num
    ''', 'eq_db_section_lines_data.csv', 5, 2, 3, 1, 0, tsid=tsid)

    csv_comparator(cpr.si, '''
        SELECT hour_num, section_id, 0
        from tsdb2.wh_eq_db_sections partition (&tsid)
        where is_impex = 1
        order by section_id, hour_num
    ''', 'eq_db_sections_impex_data.csv', 1, 0, tsid=tsid)

    csv_comparator(cpr.sli, '''
        SELECT sl.hour_num, sl.parallel_num, node_from_id, node_to_id,
        div_coef, sl.section_id
        from tsdb2.wh_eq_db_section_lines partition (&tsid) sl,
        tsdb2.wh_eq_db_sections partition (&tsid) s
        where s.is_impex = 1
        and s.hour_num = sl.hour_num
        and s.section_id = sl.section_id
        order by sl.section_id, node_from_id, node_to_id, sl.parallel_num, sl.hour_num
    ''', 'eq_db_section_lines_impex_data.csv', 5, 2, 3, 1, 0, tsid=tsid)


class CommonPreparer(object):
    """singleton class for data preservation"""
    def __init__(self):
        self.ia = None
        self.pzd = None
        self.pz_dr = None
        self.fd = None
        self.eg = None
        self.lh = None
        self.cs = None
        self.crs = None
        self.es = None
        self.ed = None
        self.eimp = None
        self.eq = None
        self.pq = None
        self.pv = None
        self.sw = None
        self.sh = None
        self.el = None
        self.s = None
        self.sl = None
        self.si = None
        self.sli = None
        self.stngs = None
        self.dr = None
        self.ps = None

    def clear(self):
        """clear properties"""
        self.__init__()

common_preparer = CommonPreparer()
