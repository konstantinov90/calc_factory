"""model test module"""
import os
import datetime
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

TEST_RUN = False
OPEN_NEW_SESSION = True
USE_VERTICA = True
SEND_TO_DB = True
SAVE_MAT_FILE = True
CALC_EQUILIBRIUM = True
CALC_TO_FINISH = True
EQUILIBRIUM_PATH = r'\\vm-tsa-app-brown\d$\MATLAB\190'
COMPARE_DATA = False

scenarios = [
    (datetime.datetime(2016, 11, 13), 25),
    (datetime.datetime(2016, 2, 13), 26),
    (datetime.datetime(2016, 3, 2), 27)
]

add_note = '_grozny_feature'


def main(calc_date, scenario, main_con, additional_note=''):
    try:
        [(tsid,)] = main_con.exec_script('''
                            SELECT trade_session_id
                            from tsdb2.trade_session
                            -- where target_Date = to_date('31012017', 'ddmmyyyy')
                            where status = 100
                            and note like 'SIPR_INIT%'
                            and target_date = :calc_date
                            ''', calc_date=calc_date)
    except ValueError:
        print('check SIPR INIT session!')
        raise

    if OPEN_NEW_SESSION and not TEST_RUN:
        # внутреннее копирование сессии
        # и разархивирование сессии
        with main_con.cursor() as curs:
            print('copying session...')
            curs.execute('''
            DECLARE
            new_session_id number;
            BEGIN
              dbms_output.enable(1000000);
              tsdb2.free_utils.g_full_diagnostic := 0;
              tsdb2.free_utils.g_need_dbms_output := 1;

              new_session_id := dp.copy_session_direct(:tsid_to_copy);

              tsdb2.bid_interface.lotus_do_resetdatafromarchiv(
                    trade_session_id_in => new_session_id,
                    stable_group_in => null,
                    slotususer_in => 'Разархивация' );

              DELETE from auto$object_actions;

              INSERT into auto$object_actions
              select IS_NEED_ARCHIV, IS_NEED_CLEAR, OBJECT_NAME, OBJECT_GROUP, OWNER,
                     OBJECT_TYPE, IS_NEED_CLEAR_ASSIST, IS_NEED_ARCHIV_ASSIST
              from tsdb2.auto$object_actions@tseur1.rosenergo.com;

              update trade_session
              set is_main = 0
              where target_date = :calc_date;

              update trade_session
              set note = 'SIPR_' || :scenario_string || :additional_note,
              is_main = 1
              where status = 1;

              commit;
            end;
            ''', tsid_to_copy=tsid, calc_date=calc_date,
                scenario_string=str(scenario), additional_note=additional_note)
            print('session copied and disarchived!')

    if not TEST_RUN:
        try: # must be open session for calc_date!
            [(open_date,)] = main_con.exec_script('''
                                SELECT target_date
                                from trade_session
                                where status = 1
                                ''')
        except ValueError:
            print('possibly no session open!')
            raise

        if open_date != calc_date:
            raise ValueError('open session has wrong date! (calc_date = %s, open_date = %s)'
                             % (calc_date.strftime('%Y-%m-%d'), open_date.strftime('%Y-%m-%d')))

    if USE_VERTICA:
        try:
            [(scenario_date,)] = DB.VerticaConnection().exec_script('''
                SELECT date_ts
                from dm_opr.model_scenarios
                where scenario_pk = :scenario
            ''', scenario=scenario)
        except ValueError:
            print('something wrong with scenario %i...' % scenario)
            raise

        if scenario_date != calc_date.date():
            raise ValueError('scenario has wrong date! (calc_date = %s, scenario_date = %s)'
                             % (calc_date.strftime('%Y-%m-%d'), scenario_date.strftime('%Y-%m-%d')))


    tdate = calc_date.strftime('%Y-%m-%d')
    print('%s%s%s' % (('tdate = %s' % tdate) if tdate else '', \
                      (', tsid = %i' % tsid) if tsid else '', \
                      (', scenario = %i' % scenario) if 'scenario_date' in locals() else ''))


    m.initialize_model(tsid, calc_date)
    if USE_VERTICA:
        m.augment_model(scenario, calc_date)
    m.intertwine_model()

    # костыли!!!!!
    if calc_date == datetime.datetime(2016, 2, 2) \
        or calc_date == datetime.datetime(2016, 11, 13) \
        or calc_date == datetime.datetime(2016, 1, 21):
        for _hd in m.Section[5078].hour_data:
            _hd.p_max = 3300
        for _hd in m.Section[5072].hour_data:
            _hd.p_max = 2700
        for _hd in m.Line.by_key[526800, 524717, 0].hour_data:
            _hd.state = True

    if calc_date == datetime.datetime(2016, 2, 13) \
        or calc_date == datetime.datetime(2016, 3, 20) \
        or calc_date == datetime.datetime(2016, 4, 14) \
        or calc_date == datetime.datetime(2016, 7, 7):
        for _hd in m.Section[5078].hour_data:
            _hd.p_max = 3300
        for _hd in m.Section[5072].hour_data:
            _hd.p_max = 2700

    for code in (12010, 12070):
        dgu = m.Dgu.by_code[code]
        for hour in range(8):
            dgu.hour_data[hour].turn_off()

    ###########################

    if SEND_TO_DB and not TEST_RUN:
        m.fill_db(calc_date)

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
            SELECT hour_num, gen_id, p_min, p, ramp_up, ramp_down,
            pnpr, pvpr, station_type, node
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
            n$node, f$volume, hr.bid_max_price*1e6, 1
            from tsdb2.wh_fixedcon_consumer partition (&tsid) f,
            tsdb2.wh_trader partition (&tsid) t,
            tsdb2.wh_hour partition (&tsid) hr
            where t.consumer2 = f.n$objectid
            and hr.hour = f.n$hour

            order by id, node_id, hour_num, interval_num
        ''', 'eq_db_demands_data.csv', 1, 3, 0, 2, tsid=tsid)

        csv_comparator(eimp, '''
            SELECT hour_num, section_number, direction, interval_num, volume, price, is_accepting
            FROM tsdb2.wh_eq_db_impexbids partition (&tsid) a
            order by section_number, hour_num, interval_num
        ''', 'eq_db_impexbids_data.csv', 1, 0, 3, tsid=tsid)

        csv_comparator(eq, '''
            SELECT hour_num, node_id, u_base, start_u, start_phase, u_rated,
            region_id, price_zone, nvl(pricezonefixed, -1), is_price_zone
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
            SELECT hour_num, node_id, U_base, u_rel, phase_start, P_start,
            q_max, q_min, is_main_for_dr
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


    def _hourize(input_data, zero_size = mat4py.ZeroSize(0)):
        """break data hour-wise"""
        HOURCOUNT = 24
        ret_data = []
        for hour in range(HOURCOUNT):
            data = [d[1:] for d in input_data if d[0] == hour]
            ret_data.append([{'InputData': [data] if len(data) > 1 else data if data else zero_size}])
        return ret_data

    def _hourize_group_constraints(group_data, constraint_data):
        """break group constraint data hour-wise"""
        HOURCOUNT = 24
        ret_data = []
        for hour in range(HOURCOUNT):
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

    if SAVE_MAT_FILE and not TEST_RUN:
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
            'ImpexBids': _hourize(eimp, mat4py.ZeroSize(5)),
            'Generators': _hourize(eg),
            'PriceZoneDemands': _hourize(pzd),
            'Fuel': {'InputData': [fd]},
            'GeneratorsDataLastHour': {'InputData': [lh]},
            'HourNumbers': [{i} for i in range(24)],
            'Settings': {'InputData': _transpose(stngs)},
            'DemandResponse': {'InputData': [dr] if dr else mat4py.ZeroSize(3)},
            'PeakSo': {'InputData': [ps]},
            'PriceZoneSettings': {'InputData': [pz_dr]}
        }

        mat4py.savemat(common_file_name, {
            'HourData': data_to_load, 'Fuel': data_to_load['Fuel']})
        # eng.cd(os.getcwd(), nargout=0)
        # eng.save_mat_file(common_file_name, (data_to_load, data_to_load['Fuel']),
        #                   ('HourData', 'Fuel'), nargout=0)

    if CALC_EQUILIBRIUM and not TEST_RUN:
        eng = matlab.engine.start_matlab()
        eng.cd(EQUILIBRIUM_PATH, nargout=0)
        if eng.fn_Run(2, os.path.join(os.getcwd(), common_file_name), nargout=1):
            print(Exception('Equilbrium error!'))
        eng.fn_Run(3, nargout=1)
        eng.quit()

    if CALC_TO_FINISH and not TEST_RUN:
        with main_con.cursor() as curs:
            print('gather statistics')
            curs.execute('''
            DECLARE
            l_owner varchar2(256) :='tsdb2';

            -- Сбор статистики
              procedure calc_stats(p_table_name varchar2)
              is
              begin
                dbms_stats.gather_table_stats(ownname => l_owner, tabname => upper(p_table_name));
              end;

            BEGIN
            for rec in (

                SELECT * from auto$object_actions t
                where upper(t.OBJECT_type) in ('TABLE')
                order by object_name
            ) loop
            calc_stats(rec.OBJECT_NAME);
            end loop;
            end;
            ''')

            print('fill netnode')
            curs.execute('''
            begin
            for rec in (
                SELECT o$na, o$ny, o$name from (
                    select rn.o$na, rn.o$ny, rn.o$name from rastr_node rn, rastr_load rl, trader gtp
                    where rn.hour = rl.hour
                    and rn.hour = 0
                    and gtp.consumer2 = rl.o$consumer
                    and rn.o$ny = rl.o$node
                    and nvl(gtp.is_unpriced_zone, 0) = 0
                    and rn.o$ny not in (select node_code from netnode)
                    group by rn.o$na, rn.o$ny, rn.o$name

                    union all

                    select rn.o$na, rn.o$ny, rn.o$name from tsdb2.rastr_generator rg,
                    tsdb2.trader gtp, tsdb2.trader rge,
                    tsdb2.rastr_node rn
                    where rg.hour = 0
                    and rg.hour = rn.hour
                    and rg.o$node = rn.o$ny
                    and rge.parent_object_id = gtp.tradeR_id
                    and rge.trader_type = 103
                    and rg.o$num = rge.tradeR_code
                    and rg.o$node not in (select node_code from tsdb2.netnode)
                    and rn.o$na not in (select n$na from tsdb2.impex_na where n$na is not null)
                    group by rn.o$na, rn.o$ny, rn.o$name
                ) group by o$na, o$ny, o$name
            ) loop

            insert into netnode
            SELECT
            (select max(rec_node_id) + 100 from netnode) rec_node_id
            , start_version, end_version, rec.o$ny node_code,
                   begin_date, end_date, node_name, rec.o$na es_ref, zsp_code,
                   is_deleted, real_node_id
              FROM netnode a
              where node_code in (
              select max(node_code) from netnode
              );
              commit;
            end loop;
            end;
            ''')

            print('aggregate')
            curs.execute('''
            declare
            rio_ver number;
            has_offtrade_area number;
            dt date;
            begin
            SELECT max(start_version) into rio_ver FROM trader;
            select target_date into dt from trade_session where status = 1;

            bid_prepare_distrib.isolated_zones;

            select
            case
                when count(*) > 0 then 1
                else 0
            end into has_offtrade_area
            from offtrade_isolated_area;

            bid_interface.lotus_do_agregat(dt,rio_ver,0.03,has_offtrade_area,0,'Агрегация');

            end;
            ''')

            print('archive')
            curs.execute('''
            begin
            bid_utils.arch_table('AGREGAT', null, null);
            bid_utils.arch_table('ASSIST', null, null);
            bid_utils.arch_table('BID', null, null);
            bid_utils.arch_table('BID2', null, null);
            bid_utils.arch_table('BID_SRC', null, null);
            bid_utils.arch_table('BILATERAL_MATRIX', null, null);
            bid_utils.arch_table('CARANA', null, null);
            bid_utils.arch_table('CONSUMER_INFO', null, null);
            bid_utils.arch_table('CSV', null, null);
            bid_utils.arch_table('DICS', null, null);
            bid_utils.arch_table('DISTR', null, null);
            bid_utils.arch_table('DISTR1', null, null);
            bid_utils.arch_table('EVR', null, null);
            bid_utils.arch_table('EVR_SRC', null, null);
            bid_utils.arch_table('FirstPoint', null, null);
            bid_utils.arch_table('GGTPS_DISTR', null, null);
            bid_utils.arch_table('HELP_IMPEX', null, null);
            bid_utils.arch_table('IASUKUT', null, null);
            bid_utils.arch_table('IMP_PR_REDUCE', null, null);
            bid_utils.arch_table('LIMITS', null, null);
            bid_utils.arch_table('MGP', null, null);
            bid_utils.arch_table('PNT', null, null);
            bid_utils.arch_table('RC_DISTR', null, null);
            bid_utils.arch_table('RD', null, null);
            bid_utils.arch_table('RD2', null, null);
            bid_utils.arch_table('REPORT', null, null);
            bid_utils.arch_table('RIO_2', null, null);
            bid_utils.arch_table('RIO__TRADER_SPEC_DATE', null, null);
            bid_utils.arch_table('TRADER', null, null);
            bid_utils.arch_table('VSVGO', null, null);
            end;
            ''')

if __name__ == '__main__':
    for tdate, scenario_num in scenarios:
        main(tdate, scenario_num, DB.OracleConnection(), add_note)
