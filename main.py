import mat4py
import matlab.engine
import os
import time
import decimal
from operator import itemgetter
from utils import DB
from utils.progress_bar import update_progress
from eq_db.classes.dpg_list import make_dpgs
from eq_db.classes.bids import make_bids
from eq_db.classes.nodes import make_nodes
from eq_db.classes.lines import make_lines
from eq_db.classes.sections import make_sections
from eq_db.classes.rge_groups import make_rge_groups
from eq_db.classes.dpg import DpgImpex
from eq_db.classes.supplies import DpgSupply
from eq_db.classes.demands import DpgDemand
from eq_db.classes.demands import DpgDemandFSK
from eq_db.classes.demands import DpgDemandSystem
from eq_db.classes.demands import DpgDemandLoad
from eq_db.classes.dgus import make_dgus
from eq_db.classes.gus import make_gus
from eq_db.classes.areas import make_areas
from eq_db.classes.consumers import make_consumers
from eq_db.classes.stations import make_stations
from eq_db.classes.loads import make_loads

from eq_db.vertica_corrections import add_stations_vertica
from eq_db.vertica_corrections import add_gus_vertica
from eq_db.vertica_corrections import add_dgus_vertica
from eq_db.vertica_corrections import add_supplies_vertica
from eq_db.vertica_corrections import add_nodes_vertica
from eq_db.vertica_corrections import add_lines_vertica
from eq_db.vertica_corrections import add_bids_vertica


from sql_scripts import SettingsScript

con = DB.OracleConnection()
con_v = DB.VerticaConnection()

for scenario, tdate in con_v.exec_script('''select scenario_pk, date_ts
                            from dm_opr.model_scenarios
                            where scenario_pk = 6
                            order by date_ts'''):

    print('scenario #%i || target date = %s' % (scenario, tdate), end=' ')

    for tsid, td in con.exec_script('''select trade_session_id, target_date--, note
                               from tsdb2.trade_Session
                               where target_date = :tdate
                               and is_main = 1
                               order by target_date''', {'tdate': tdate}):

        # tdate = td.date()
        print('|| trade_session_id = %i' % tsid)

    common_file_name = 'common1_%s.mat' % tdate
    settings_file_name = 'settings.mat'
    eq_folder = r'\\vm-tsa-app-brown\d$\MATLAB\178'

    start_time = time.time()


    settings = con.exec_script(SettingsScript().get_query(), {'tsid': tsid})
    mat4py.savemat('settings.mat', {
        's1': [d[0] for d in settings],
        's2': [d[1] for d in settings],
        's3': [d[2] if d[2] else 'null' for d in settings],
        's4': [float(d[3]) for d in settings],
        's5': [float(d[4]) for d in settings]
    })

    dgus = add_dgus_vertica(make_dgus(tsid, tdate), scenario, tdate)
    gus = add_gus_vertica(make_gus(tsid, tdate), scenario, tdate)
    stations = add_stations_vertica(make_stations(tsid, tdate), scenario, tdate)

    areas = make_areas(tsid, tdate)
    consumers = make_consumers(tsid, tdate)
    loads = make_loads(tsid, tdate)
    rge_groups = make_rge_groups(tsid, tdate)

    nodes = add_nodes_vertica(make_nodes(tsid, tdate), scenario, tdate)
    lines = add_lines_vertica(make_lines(tsid, tdate), scenario, tdate)

    sections = make_sections(tsid, tdate, lines)

    dpgs = add_supplies_vertica(make_dpgs(tsid, tdate), scenario, tdate)
    bids = add_bids_vertica(make_bids(tsid, tdate), scenario, tdate)


    lines.attach_nodes(nodes)
    gus.set_parent_dgus(dgus)
    dgus.set_to_remove()
    dgus.set_nodes(nodes)
    dgus.set_parent_dpgs(dpgs)
    loads.attach_nodes(nodes)

    dpgs.set_stations(stations)
    dpgs.set_bids(bids)
    dpgs.attach_sections(sections)
    dpgs.set_areas(areas)
    dpgs.set_loads(loads)
    dpgs.set_consumers(consumers)
    nodes.attach_dpgs(dpgs)
    dpgs.calculate_node_load()
    dpgs.calculate_dpg_node_load()
    areas.attach_nodes(nodes)

    nodes.set_balance_nodes(lines)

    def hourize_data(data):
        HOURCOUNT = 24
        ret = [[] for hour in range(0, HOURCOUNT)]
        for i, e in enumerate(data):
            ret[e[0]].append(e[1:])
            update_progress((i + 1) / len(data))
        return ret
    dim = [d if d else [(0,)] for d in dpgs.get_prepared_impex_bids_data()]


    # data_to_load = {'HourData': {
    #     'Nodes': {'InputData': nodes.get_prepared_nodes_data()},
    #     'NodesPQ': {'InputData': nodes.get_prepared_nodes_pq_data()},
    #     'NodesPV': {'InputData': nodes.get_prepared_nodes_pv_data()},
    #     'NodesSW': {'InputData': nodes.get_prepared_nodes_sw_data()},
    #     'Shunts': {'InputData': nodes.get_prepared_shunts_data()},
    #     'Lines': {'InputData': lines.get_prepared_lines_data()},
    #     'GroupConstraints': {'InputData': rge_groups.get_prepared_constraints_data()},
    #     'GroupConstraintsRges': {'InputData': rge_groups.get_prepared_constraint_rges_data()},
    #     'Sections': {'InputData': sections.get_prepared_sections_data()},
    #     'SectionLines': {'InputData': sections.get_prepared_sections_lines_data()},
    #     'SectionsImpex': {'InputData': sections.get_prepared_sections_impex_data()},
    #     'SectionLinesImpex': {'InputData': sections.get_prepared_sections_lines_impex_data()},
    #     'SectionRegions': {'InputData': [sections.get_prepared_impex_area_data()]},
    #     'Demands': {'InputData': dpgs.get_prepared_demands_data()},
    #     'Supplies': {'InputData': dpgs.get_prepared_supplies_data()},
    #     'ImpexBids': {'InputData': dim},
    #     'Generators': {'InputData': dgus.get_prepared_generator_data()},
    #     'PriceZoneDemands': {'InputData': dpgs.get_prepared_price_zone_consumption()},
    #     'Fuel': {'InputData': [dpgs.get_prepared_fuel_data()]},
    #     'GeneratorsDataLastHour': {'InputData': [dpgs.get_prepared_generator_last_hour_data()]}
    # }}
    # # print(dpgs[151630].code, dpgs[151630].is_pintsch_gas)
    # print('Total time = %.2f seconds' % (time.time() - start_time))
    #
    # mat4py.savemat(common_file_name, data_to_load)
    #
    # print('Total time + common.mat = %.2f seconds' % (time.time() - start_time))
    #
    # eng = matlab.engine.start_matlab()
    # eng.cd(os.getcwd())
    # eng.correct_common(common_file_name, settings_file_name, eq_folder, nargout=0)
    #
    # print('Total time + correct common = %.2f seconds' % (time.time() - start_time))

    # eng.cd(eq_folder)
    # eng.fn_Run(2, common_file_name, nargout=0)
    # # eng.fn_Run(3, nargout=0)
