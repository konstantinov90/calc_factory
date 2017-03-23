"""model test module"""
import os
import datetime
import shutil
from itertools import product
import settings as S
from eq_db import model as m, common_mat_preparer as cmp
from equil_service import run_equilibrium
from utils import DB
from reports import generate_report
from sql_scripts import report_closed_sections as rcs, report_full_analysis as rfa, \
report_region_prices as rrp, report_consolidated as rc, report_generators_reloading as rgr


TEST_RUN = False
OPEN_NEW_SESSION = True
USE_VERTICA = True
SEND_TO_DB = True
SAVE_MAT_FILE = True
CALC_EQUILIBRIUM = True
CALC_TO_FINISH = True
COMPARE_DATA = False
MAKE_REPORTS = True

sipr_calc_length = 100

SCENARIOS = [6, 7]

CALCS = [
    # (datetime.datetime(2016, 1, 21), 1),
    (datetime.datetime(2016, 2, 2), 2),
    (datetime.datetime(2016, 2, 13), 3),
    (datetime.datetime(2016, 3, 20), 4),
    (datetime.datetime(2016, 4, 14), 5),
    (datetime.datetime(2016, 7, 7), 6),
    (datetime.datetime(2016, 8, 1), 7),
    # (datetime.datetime(2016, 8, 20), 8),
    (datetime.datetime(2016, 10, 10), 9),
    (datetime.datetime(2016, 11, 13), 10)
]


def main(calc_date, sipr_calc, main_con, scenario):
    """main function body"""
    additional_note = '_scenario_%i' % scenario

    try: # check SIPR_INIT session
        [(tsid_init,)] = main_con.exec_script('''
                            SELECT trade_session_id
                            from tsdb2.trade_session
                            where status = 100
                            and note like 'SIPR_INIT%'
                            and target_date = :calc_date
                            ''', calc_date=calc_date)
    except ValueError:
        print('check SIPR INIT session!')
        raise

    if USE_VERTICA:
        vertica_con = DB.VerticaConnection()
        try: # check vertica data
            [(scenario_date, future_date)] = vertica_con.exec_script('''
                                            SELECT date_ts, date_model
                                            from dm_opr.model_scenarios
                                            where scenario_pk = :sipr_calc
                                            ''', sipr_calc=sipr_calc)
        except ValueError:
            print('something wrong with SIPR calc data %i...' % sipr_calc)
            raise

        if scenario_date != calc_date.date():
            raise ValueError('SIPR calc has wrong date! (calc_date = %s, scenario_date = %s)'
                             % (calc_date.strftime('%Y-%m-%d'), scenario_date.strftime('%Y-%m-%d')))

    cdate_str = calc_date.strftime('%Y-%m-%d')
    print('%s%s%s' % (('calc_date = %s' % cdate_str) if cdate_str else '', \
    (', tsid_init = %i' % tsid_init) if tsid_init else '', \
    (', sipr_calc = %i' % sipr_calc) if USE_VERTICA else ''))

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
              where target_date = :future_date;

              update trade_session
              set note = 'SIPR_' || :sipr_calc || :additional_note,
              date_start = :date_start,
              is_main = 1
              where status = 1;

              commit;
            end;''', tsid_to_copy=tsid_init, future_date=future_date,
                         date_start=datetime.datetime.now(),
                         sipr_calc='{}{:02}'.format(scenario, sipr_calc % sipr_calc_length),
                         additional_note=additional_note)
            print('session copied and disarchived!')

    if not TEST_RUN:
        try: # must be open session for calc_date!
            [(open_date, tsid)] = main_con.exec_script('''
                                SELECT target_date, trade_session_id
                                from trade_session
                                where status = 1
                                ''')
        except ValueError:
            print('possibly no session open!')
            raise

        if open_date != calc_date:
            raise ValueError('open session has wrong date! (calc_date = %s, open_date = %s)'
                             % (calc_date.strftime('%Y-%m-%d'), open_date.strftime('%Y-%m-%d')))



    m.initialize_model(tsid_init, calc_date)
    if USE_VERTICA:
        m.augment_model(sipr_calc, calc_date)

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

    if scenario == 5:
        for code in (12010, 12070):
            dgu = m.Dgu.by_code[code]
            for hour in range(8):
                dgu.hour_data[hour].turn_off()

    ###########################
    if not TEST_RUN:
        m.Setting['Session_Id'].string_value = '_d%s_ts%i_sipr%i' \
            % (datetime.datetime.now().strftime('%Y%m%d%H%M%S'), tsid, sipr_calc)

    m.intertwine_model()

    if SEND_TO_DB and not TEST_RUN:
        m.fill_db(calc_date)

    cmp.prepare_data_for_common()

    if COMPARE_DATA:
        cmp.compare_data(tsid_init)

    common_file_name = 'common_%s_%s.mat' % ('v' if USE_VERTICA else '1', cdate_str)
    print(common_file_name)

    if SAVE_MAT_FILE and not TEST_RUN:
        cmp.save_mat_file(common_file_name)

    if CALC_EQUILIBRIUM and not TEST_RUN:
        run_equilibrium(os.path.join(os.getcwd(), common_file_name))

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

            print('finish session')
            curs.execute('''
            begin

            UPDATE trade_session
            set status = 100,
            date_finish = :date_finish,
            target_date = :future_date
            where status = 1;

            commit;

            end;
            ''', date_finish=datetime.datetime.now(), future_date=future_date)

            if USE_VERTICA:
                with vertica_con.cursor()  as curs:
                    curs.execute('''
                        UPDATE dm_opr.model_scenarios
                        set trade_session_id = {tsid}
                        where scenario_pk = {sipr_calc}
                    '''.format(tsid=tsid, sipr_calc=sipr_calc))
                    vertica_con.commit()

    if MAKE_REPORTS and not TEST_RUN:
        with main_con.cursor() as curs:
            print('fill sipr_calculation_result')
            curs.execute(r'''
            declare
            n_is_main number := 1;

            s_note varchar2(1000);
            n_scenario number;
            n_sipr number;

            begin

            SELECT note into s_note
            from trade_Session
            where target_date = :tdate
            and is_main = n_is_main;

            n_scenario := to_number(REGEXP_SUBSTR(s_note,'\d$'));
            n_sipr := to_number(REGEXP_SUBSTR(s_note,'\d+'));

            DELETE from sd$calculation_result
            where target_date = :tdate;

            DELETE from sipr_calculation_result
            where target_date = :tdate
            and sipr_calc = n_sipr
            and scenario = n_scenario;

            commit;

            sd$support_department.fill_calculation_results(:tdate, n_is_main, TRUE);

            INSERT into sipr_calculation_result
            SELECT n_sipr, n_scenario, s_note note, c.*
            from sd$calculation_result c
            where target_date = :tdate;

            commit;

            end;
            ''', tdate=future_date)

            print('fill tmp_sipr_report_generators_rge')
            curs.execute('''
            begin

            DELETE from tmp_sipr_report_generators_rge
            where sipr_calc = :sipr_calc
            and scenario = :scenario;

            commit;

            INSERT into tmp_sipr_report_generators_rge

            SELECT :tdate, :scenario, 'SIPR_' || :sipr_calc || :additional_note,
                :tsid_init, :tsid,
            nvl(new.participant_code, old.participant_code) participant_code,
            nvl(new.prt_name,old.prt_name) prt_name,
            nvl(new.station_code, old.station_code) station_code,
            nvl(old.station_category, new.station_category) station_category,
            nvl(old.station_type, new.station_type) station_type,
            nvl(old.name, new.name) name,
            nvl(old.is_unpriced_zone, new.is_unpriced_zone) is_unpriced_zone,
            nvl(new.oes, old.oes) oes,
            nvl(new.gtp_code, old.gtp_code) gtp_code,
            nvl(new.gtp_name, old.gtp_name) gtp_name,
            nvl(new.rge_code, old.rge_code) rge_code,
            nvl(new.hour, old.hour) hour,
            old.pmin old_pmin, old.pmax old_pmax, old.tg old_tg, old.pr_tg old_pr_tg,
            new.pmin new_pmin, new.pmax new_pmax, new.tg new_tg, new.pr_tg new_pr_tg,
            nvl(new.pmin,0) - nvl(old.pmin,0) pmin_diff, nvl(new.pmax,0) - nvl(old.pmax,0) pmax_diff,
            nvl(new.tg,0) - nvl(old.tg,0) tg_diff,
            nvl(new.pr_tg,0) - nvl(old.pr_tg,0) diff_pr,
            :sipr_calc
            from (
                 select rg.hour, st.begin_date,
                        prt.trader_code participant_code,
                        prt.full_name prt_name,
                        st.trader_code station_code,
                        st.station_category,
                        decode(st.station_type, 1, 'ТЭС', 2, 'ГЭС', 3, 'АЭС',
                            4, 'ТЭС', 5, 'СЭС', 6, 'ВЭС') station_type,
                        nvl(st.full_name, max(gtp.full_name)) name,
                        gtp.is_unpriced_zone, gtp.oes,
                        gtp.trader_code gtp_code, gtp.full_name gtp_name,
                        rg.o$num rge_code, nvl(sum(s.f$volume), 0) tg,
                        nvl(max(s.f$price), 0) pr_tg,
                        o$pmin pmin, o$pmax pmax
                  from
                  (select * from wh_trader where trade_Session_id = :tsid) st,
                  (select * from wh_trader where trade_Session_id = :tsid) prt,
                  (select * from wh_trader where trade_Session_id = :tsid) gtp,
                  (select * from wh_trader where trade_Session_id = :tsid) rge,
                  (select * from wh_carana$sout where trade_Session_id = :tsid) s,
                  (select * from wh_rastr_generator where trade_Session_id = :tsid) rg
                  where st.trader_id = gtp.dpg_station_id
                  and prt.trader_id = st.parent_object_id
                  and gtp.trader_id = rge.parent_object_id
                  and rge.trader_type = 103
                  and rge.trader_code = rg.o$num
                  and s.n$hour(+) = rg.hour
                  and rg.o$num = s.n$objectid(+)
                  group by rg.hour, st.begin_date, st.trader_code, prt.trader_code,
                  prt.full_name, st.full_name, st.station_type, st.station_category,
                  gtp.is_unpriced_zone, gtp.oes, o$pmin, o$pmax, gtp.trader_code, gtp.full_name,
                  rg.o$num
            ) new full join (
                 select rg.hour, st.begin_date,
                        prt.trader_code participant_code,
                        prt.full_name prt_name,
                        st.trader_code station_code,
                        st.station_category,
                        decode(st.station_type, 1, 'ТЭС', 2, 'ГЭС', 3, 'АЭС',
                            4, 'ТЭС', 5, 'СЭС', 6, 'ВЭС') station_type,
                        nvl(st.full_name, max(gtp.full_name)) name,
                        gtp.is_unpriced_zone, gtp.oes,
                        gtp.trader_code gtp_code, gtp.full_name gtp_name,
                        rg.o$num rge_code, nvl(sum(s.f$volume), 0) tg,
                        nvl(max(s.f$price), 0) pr_tg,
                        o$pmin pmin, o$pmax pmax
                  from
                  (select * from wh_trader where trade_Session_id = :tsid_init) st,
                  (select * from wh_trader where trade_Session_id = :tsid_init) prt,
                  (select * from wh_trader where trade_Session_id = :tsid_init) gtp,
                  (select * from wh_trader where trade_Session_id = :tsid_init) rge,
                  (select * from wh_carana$sout where trade_Session_id = :tsid_init) s,
                  (select * from wh_rastr_generator where trade_Session_id = :tsid_init) rg
                  where st.trader_id = gtp.dpg_station_id
                  and prt.trader_id = st.parent_object_id
                  and gtp.trader_id = rge.parent_object_id
                  and rge.trader_type = 103
                  and rge.trader_code = rg.o$num
                  and s.n$hour(+) = rg.hour
                  and rg.o$num = s.n$objectid(+)
                  group by rg.hour, st.begin_date, st.trader_code, prt.trader_code,
                  prt.full_name, st.full_name, st.station_type, st.station_category,
                  gtp.is_unpriced_zone, gtp.oes, o$pmin, o$pmax, gtp.trader_code, gtp.full_name,
                  rg.o$num
            ) old
            on new.station_code = old.station_code
            and new.rge_code = old.rge_code
            and new.hour = old.hour;

            commit;
            end;
            ''', tdate=future_date, scenario=scenario, additional_note=additional_note,
                         tsid=tsid, tsid_init=tsid_init,
                         sipr_calc='{}{:02}'.format(scenario, sipr_calc % sipr_calc_length))

        new_ts = DB.Partition(tsid)
        old_ts = DB.Partition(tsid_init)
        reports = [
            (rgr, 'generators_reloading.xlsx', 'Лист1', (3, 1),
             {'tsid': tsid, 'tsid_init': tsid_init, 'tdate': future_date}),
            (rc, 'consolidated_report.xlsx',
             ('Hourly data_eur', 'Hourly data_eur', 'Hourly data_sib', 'Hourly data_sib'),
             ((7, 1), (35, 1), (7, 1), (35, 1)),
             ({'tsid': tsid, 'pz': 1}, {'tsid': tsid_init, 'pz': 1},
              {'tsid': tsid, 'pz': 2}, {'tsid': tsid_init, 'pz': 2})
            ),
            (rcs, 'closed_sections.xls', 'запертые сечения', (2, 1),
             {'tsid': new_ts}),
            (rfa, 'full_analysis.xlsx', 'Анализ_new', (3, 1),
             {'tdate': future_date, 'tdate_init': calc_date.date(),
              'sipr_calc': sipr_calc, 'scenario': scenario}),
            (rrp, 'region_prices.xlsx', 'БД', (2, 1),
             {'tsid': new_ts, 'tsid_init': old_ts, 'tdate': future_date})
        ]

        for script, template, worksheet, start, kwargs in reports:
            filename = 'sipr{}{:02}_{}_{}'.format(
                scenario, sipr_calc % sipr_calc_length, future_date.strftime('%Y%m%d'), template)
            file_path = os.path.join(r'C:\python\calc_factory\reports', filename)
            generate_report(main_con, script, template, worksheet, start, file_path, kwargs)
            reports_path = os.path.join(S.REPORTS_PATH, \
                    'sipr{}{:02}'.format(scenario, sipr_calc % sipr_calc_length))
            if not os.path.isdir(reports_path):
                os.mkdir(reports_path)
            shutil.copy(file_path, os.path.join(reports_path, filename))

if __name__ == '__main__':
    with DB.OracleConnection().cursor() as cursor:
        cursor.execute('''
        begin
            UPDATE loader_out_tab
            set for_main_eq = 0
            where code = 'run CONVERTOUT1';

            commit;
        end;
        ''')
    try:
        for (target_date, sipr), scen in product(CALCS, SCENARIOS):
            main(target_date, sipr + sipr_calc_length * scen, DB.OracleConnection(), scen)
    except Exception:
        raise
    finally:
        with DB.OracleConnection().cursor() as cursor:
            cursor.execute('''
            begin
                UPDATE loader_out_tab
                set for_main_eq = 1
                where code = 'run CONVERTOUT1';

                commit;
            end;
            ''')
