"""model test module"""
import os
from operator import itemgetter, attrgetter
try:
    import matlab
    import matlab.engine
except ImportError as ex:
    print(ex)
import mat4py
from eq_db import model as m
from utils import DB
from utils.csv_comparator import csv_comparator


USE_VERTICA = True
SAVE_MAT_FILE = True
CALC_EQUILIBRIUM = True
EQUILIBRIUM_PATH = r'\\vm-tsa-app-brown\d$\MATLAB\189'
COMPARE_DATA = False



for target_date, tsid in DB.OracleConnection().exec_script('''
                        SELECT target_date, decode(status, 1, null, trade_session_id)
                        from tsdb2.trade_session
                        --where target_Date = to_date('31072016', 'ddmmyyyy')
                        where status = 1
                        and is_main = 1
                        '''):
    for scenario, *_ in DB.VerticaConnection().exec_script('''
                            SELECT distinct scenario_pk
                            from dm_opr.model_scenarios
                            --where date_ts = :tdate
                            --and scenario_pk > 6
                            where scenario_pk = 6
                            order by scenario_pk''', tdate=target_date):

        tdate = target_date.strftime('%Y-%m-%d')
        print('tdate = %s, tsid = %r, scenario = %i' % (tdate, tsid, scenario))

        m.initialize_model(tsid, scenario, target_date, USE_VERTICA)
        m.intertwine_model()
        if USE_VERTICA:
            m.fill_db()

        ia = [tuple(i if i else 0 for i in impex_area.get_impex_area_data())
              for impex_area in m.ImpexArea
              if impex_area.get_impex_area_data()[0]]

        pzd = []
        pz_dr = []
        for price_zone in m.PriceZone:
            pzd += price_zone.get_consumption()
            pz_dr.append(price_zone.get_settings())

        fd = [wsum.get_fuel_data() for wsum in m.Wsumgen]

        eg = []
        for dgu in m.Dgu:
            eg += dgu.get_prepared_generator_data()

        lh = [dgu.get_last_hour_data() for dgu in m.Dgu if dgu.get_last_hour_data()]

        cs = []
        crs = []
        for dgu_group in m.DguGroup:
            cs += dgu_group.get_constraint_data()
            crs += dgu_group.get_dgu_data()

        es = []
        for dpg in m.DpgSupply:
            es += dpg.get_distributed_bid()

        ed = []
        for dpg in m.DpgDemand:
            ed += dpg.get_distributed_bid()

        eimp = []
        for dpg in m.DpgImpex:
            eimp += dpg.get_distributed_bid()

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

        el = []
        for line in m.Line:
            el += line.get_eq_db_lines_data()

        s = []
        sl = []
        si = []
        sli = []
        for section in m.Section:
            s += section.get_section_data()
            sl += section.get_section_lines_data()
            si += section.get_section_impex_data()
            sli += section.get_section_lines_impex_data()

        stngs = [setting.get_settings_data() for setting in m.Setting]

        dr = []
        for dpg in m.DpgDemand:
            dr += dpg.get_demand_response_data()

        ps = [peak_so.get_peak_so_data() for peak_so in m.PeakSO]

        if COMPARE_DATA:
            ia.sort(key=itemgetter(2, 0, 1))
            pzd.sort(key=itemgetter(0, 1))
            fd.sort(key=itemgetter(0, 1))
            eg.sort(key=itemgetter(1, 0))
            lh.sort(key=itemgetter(0))
            cs.sort(key=itemgetter(1, 0))
            crs.sort(key=itemgetter(1, 0, 2))
            es.sort(key=itemgetter(5, 0, 6))
            ed.sort(key=itemgetter(2, 4, 0, 3))
            eimp.sort(key=itemgetter(1, 0, 3))
            el.sort(key=itemgetter(1, 2, 3, 0))
            s.sort(key=itemgetter(1, 0))
            sl.sort(key=itemgetter(5, 2, 3, 1, 0))
            si.sort(key=itemgetter(1, 0))
            sli.sort(key=itemgetter(5, 2, 3, 1, 0))

            csv_comparator(ia, '''
                SELECT section_id, nvl(region_id, 0), price_zone
                from tsdb2.wh_eq_db_section_regions partition (&tsid)
                where section_id != 0
                order by price_zone, section_id, region_id
            ''', 'eq_db_section_regions_data.csv', 2, 0, 1, tsid=tsid)

            csv_comparator(pzd, '''
                SELECT a.hour_num, a.price_zone, a.p_cons_sum_value
                FROM tsdb2.wh_eq_db_price_zone_sum_values partition (&tsid) a
                order by hour_num, price_zone
            ''', 'eq_db_price_zone_sum_values_data.csv', 1, 0, tsid=tsid)

            csv_comparator(fd, '''
                SELECT id, gen_id, p_min, p_max, hour_start, hour_end
                FROM tsdb2.wh_eq_db_fuel partition (&tsid) a
                order by id, gen_id
            ''', 'eq_db_fuel_data.csv', 0, 1, tsid=tsid)

            csv_comparator(eg, '''
                SELECT hour_num, gen_id, p_min, p, ramp_up, ramp_down
                from tsdb2.wh_eq_db_generators partition (&tsid)
                order by gen_id, hour_num
            ''', 'eq_db_generators_data.csv', 1, 0, tsid=tsid)

            csv_comparator(lh, '''
                SELECT gen_id, pgenlasthour
                from tsdb2.wh_eq_db_generators_data partition (&tsid)
                order by gen_id
            ''', 'eq_db_generators_last_hour_data.csv', 0, tsid=tsid)

            csv_comparator(cs, '''
                SELECT hour_num, group_id, p_max, p_min
                from tsdb2.wh_eq_db_group_constraints partition (&tsid)
                where group_id <> 0
                order by group_id, hour_num
            ''', 'eq_db_constraints_data.csv', 1, 0, tsid=tsid)

            csv_comparator(crs, '''
                SELECT hour_num, group_id, gen_id
                from tsdb2.wh_eq_db_group_constraint_rges partition (&tsid)
                where group_id <> 0
                order by group_id, hour_num, gen_id
            ''', 'eq_db_constraint_rge_data.csv', 1, 0, 2, tsid=tsid)

            csv_comparator(es, '''
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

            csv_comparator([e[:1]+e[2:] for e in ed], '''
                SELECT hour_num, id, interval_num, node_id, volume, price, nvl(is_accepted,0)
                from tsdb2.wh_eq_db_demands partition (&tsid)

                union all

                select n$hour, n$objectid,
                case when trader_code in ('PCHITAZN','PAMUREZN') then -55
                     when is_system = 1 and fed_Station = 1 then -52
                     when is_system = 0 and fed_station = 1 then -42
                     when is_gaes = 1 then -32 end interval_num,
                n$node, f$volume, 9679*1e6, 1
                from tsdb2.wh_fixedcon_consumer partition (&tsid) f,
                tsdb2.wh_trader partition (&tsid) t
                where t.consumer2 = f.n$objectid
                order by id, node_id, hour_num, interval_num
            ''', 'eq_db_demands_data.csv', 1, 3, 0, 2, tsid=tsid)

            csv_comparator(eimp, '''
                SELECT hour_num, section_number, direction, interval_num, volume, price, is_accepting
                FROM tsdb2.wh_eq_db_impexbids partition (&tsid) a
                order by section_number, hour_num, interval_num
            ''', 'eq_db_impexbids_data.csv', 1, 0, 3, tsid=tsid)

            csv_comparator(eq, '''
                SELECT hour_num, node_id, u_base, start_u, start_phase, u_rated,
                region_id, price_zone, nvl(pricezonefixed, -1)
                FROM tsdb2.wh_eq_db_nodes PARTITION (&tsid)
                ORDER BY node_id, hour_num
            ''', 'eq_db_nodes_data.csv', 1, 0, tsid=tsid)

            csv_comparator(pq, '''
                SELECT hour_num, node_id, u_base, p_cons_minus_gen,
                q_cons, q_gen, u_max, u_min, cons, gen
                from tsdb2.wh_eq_db_nodes_pq partition (&tsid)
                order by node_id, hour_num
            ''', 'eq_db_nodes_pq_data.csv', 1, 0, tsid=tsid)

            csv_comparator(pv, '''
                SELECT hour_num, node_id, u_base, p_gen, q_cons,
                q_gen, type, U, q_max, q_min, U_max, u_min, cons, gen
                FROM tsdb2.wh_eq_db_nodes_pv PARTITION (&tsid)
                ORDER BY node_id, hour_num
            ''', 'eq_db_nodes_pv_data.csv', 1, 0, tsid=tsid)

            csv_comparator(sw, '''
                SELECT hour_num, node_id, U_base, u_rel, phase_start, P_start, q_max, q_min
                FROM tsdb2.wh_eq_db_nodes_sw PARTITION (&tsid)
                ORDER BY node_id, hour_num
            ''', 'eq_db_nodes_sw_data.csv', 1, 0, tsid=tsid)

            csv_comparator(sh, '''
                SELECT hour_num, node_id, U_base, g, b
                FROM tsdb2.wh_eq_db_shunts PARTITION (&tsid)
                ORDER BY node_id, hour_num
            ''', 'eq_db_shunts_data.csv', 1, 0, tsid=tsid)

            csv_comparator(el, '''
                SELECT hour_num, node_from_id, node_tO_id, parallel_num, type,
                u_base, base_coef, r,x,g,b,ktr, lagging, b_begin, b_end
                FROM tsdb2.wh_eq_db_lines PARTITION (&tsid)
                ORDER BY node_from_id, node_tO_id, parallel_num, hour_num
            ''', 'eq_db_lines_data.csv', 1, 2, 3, 0, tsid=tsid)

            csv_comparator(s, '''
                SELECT hour_num, section_id, p_max_forward, p_max_backward
                from tsdb2.wh_eq_db_sections partition (&tsid)
                where is_impex = 0
                order by section_id, hour_num
            ''', 'eq_db_sections_data.csv', 1, 0, tsid=tsid)

            csv_comparator(sl, '''
                SELECT sl.hour_num, sl.parallel_num, node_from_id, node_to_id,
                div_coef, sl.section_id
                from tsdb2.wh_eq_db_section_lines partition (&tsid) sl,
                tsdb2.wh_eq_db_sections partition (&tsid) s
                where s.is_impex = 0
                and s.hour_num = sl.hour_num
                and s.section_id = sl.section_id
                order by sl.section_id, node_from_id, node_to_id, sl.parallel_num, sl.hour_num
            ''', 'eq_db_section_lines_data.csv', 5, 2, 3, 1, 0, tsid=tsid)

            csv_comparator(si, '''
                SELECT hour_num, section_id, 0
                from tsdb2.wh_eq_db_sections partition (&tsid)
                where is_impex = 1
                order by section_id, hour_num
            ''', 'eq_db_sections_impex_data.csv', 1, 0, tsid=tsid)

            csv_comparator(sli, '''
                SELECT sl.hour_num, sl.parallel_num, node_from_id, node_to_id,
                div_coef, sl.section_id
                from tsdb2.wh_eq_db_section_lines partition (&tsid) sl,
                tsdb2.wh_eq_db_sections partition (&tsid) s
                where s.is_impex = 1
                and s.hour_num = sl.hour_num
                and s.section_id = sl.section_id
                order by sl.section_id, node_from_id, node_to_id, sl.parallel_num, sl.hour_num
            ''', 'eq_db_section_lines_impex_data.csv', 5, 2, 3, 1, 0, tsid=tsid)


        def _hourize(input_data):
            """break data hour-wise"""
            max_hour = max(d[0] for d in input_data)
            ret_data = []
            for hour in range(max_hour + 1):
                data = [d[1:] for d in input_data if d[0] == hour]
                ret_data.append([{'InputData': [data] if len(data) > 1 else data}] if data else [])
            return ret_data

        def _hourize_group_constraints(group_data, constraint_data):
            """break group constraint data hour-wise"""
            max_hour = max(d[0] for d in group_data)
            ret_data = []
            for hour in range(max_hour + 1):
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

        def _transpose(input_data):
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

        common_file_name = 'common_%s_%s.mat' % ('v' if USE_VERTICA else '1', tdate)
        print(common_file_name)

        if SAVE_MAT_FILE:
            data_to_load = {
                'Nodes': _hourize(eq),
                'NodesPQ': _hourize(pq),
                'NodesPV': _hourize(pv),
                'NodesSW': _hourize(sw),
                'Shunts': _hourize(sh),
                'Lines': _hourize(el),
                'GroupConstraints': _hourize_group_constraints(cs, crs),
                'GroupConstraintsRges': _hourize(crs),
                'Sections': _hourize(s),
                'SectionLines': _hourize(sl),
                'SectionsImpex': _hourize(si),
                'SectionLinesImpex': _hourize(sli),
                'SectionRegions': {'InputData': [ia]},
                'Demands': _hourize(ed),
                'Supplies': _hourize(es),
                'ImpexBids': _hourize(eimp),
                'Generators': _hourize(eg),
                'PriceZoneDemands': _hourize(pzd),
                'Fuel': {'InputData': [fd]},
                'GeneratorsDataLastHour': {'InputData': [lh]},
                'HourNumbers': [{i} for i in range(24)],
                'Settings': {'InputData': _transpose(stngs)},
                'DemandResponse': {'InputData': [dr]},
                'PeakSo': {'InputData': [ps]},
                'PriceZoneSettings': {'InputData': [pz_dr]}
            }

            mat4py.savemat(common_file_name, {
                'HourData': data_to_load, 'Fuel': data_to_load['Fuel']})
            # eng.cd(os.getcwd(), nargout=0)
            # eng.save_mat_file(common_file_name, (data_to_load, data_to_load['Fuel']),
            #                   ('HourData', 'Fuel'), nargout=0)

        if CALC_EQUILIBRIUM:
            eng = matlab.engine.start_matlab()
            eng.cd(EQUILIBRIUM_PATH, nargout=0)
            if eng.fn_Run(2, os.path.join(os.getcwd(), common_file_name), nargout=1):
                print(Exception('Equilbrium error!'))
            eng.fn_Run(3, nargout=1)
            eng.quit()
