"""Create Gu instances."""
import csv
import math
from operator import attrgetter
from utils import DB
# from utils.printer import print
from utils.trade_session_manager import ts_manager
from sql_scripts import gus_script as gs, nblock_script as ns, gus_script_v as gs_v, \
nblock_script_v as ns_v, block_out_v as bo_v, bid_factor_script as bfs, vsvgo_script as vs
from .gus import Gu
from .gus_hour_data import GuHourData


@ts_manager
def make_gus(tsid):
    """create Gu instances"""
    con = DB.OracleConnection()
    Gu.clear()

    for new_row in con.script_cursor(gs, tsid=DB.Partition(tsid)):
        Gu(new_row)

    for new_row in con.script_cursor(ns, tsid=DB.Partition(tsid)):
        gu_code = new_row.gu_code
        if not Gu.by_code[gu_code]:
            Gu.from_nblock(new_row)
            print('WARNING!! added Gu %i not from RIO!' % gu_code)
        unit_hd = GuHourData(new_row)
        for unit in Gu.by_code[gu_code]:
            unit.add_gu_hour_data(new_row, unit_hd)

# def _blocks_reader(scenario):
#     with open(r'vsvgo\blocks_scenario_%i.csv' % scenario, newline='') as csvfile:
#         reader = csv.reader(csvfile, delimiter=';')
#         for row in reader:
#             gu_code, hour, state = (int(i) for i in row)
#             state = bool(state)
#             yield gu_code, hour, state

@ts_manager
def add_gus_vertica(scenario):
    """add Gus instances from Vertica DB"""
    con = DB.VerticaConnection()
    for new_row in con.script_cursor(gs_v, scenario=scenario):
        Gu(new_row, is_new=True)

    for new_row in con.script_cursor(ns_v, scenario=scenario):
        Gu.by_id[new_row.gu_code].add_gu_hour_data(new_row, GuHourData(new_row))

    # факторизация заявок ГТПГ
    for fuel_type, factor in con.script_cursor(bfs, scenario=scenario):
        for g_unit in Gu:
            if fuel_type in g_unit.fuel_type_list:
                g_unit.bid_factor = factor

    # load turned off blocks from vertica
    # for gu_code, *_ in con.script_cursor(bo_v, scenario=scenario):
    #     try:
    #         for _gu in Gu.by_code[gu_code]:
    #             for gu_hd in _gu.hour_data:
    #                 gu_hd.changed = gu_hd.state
    #                 gu_hd.state = False
    #     except TypeError:
    #         print('Attempted to access nonexistent Gu %i!' % gu_code)

    # get new blocks' states from vsvgo
    # for gu_code, hour, state in _blocks_reader(scenario):
    for gu_code, hour, state in con.script_cursor(vs, scenario=scenario):
        try:
            [g_unit] = Gu.by_code[gu_code]
        except TypeError:
            print('changed Gu %i is blocked!' % gu_code)
            raise
        #     #     raise
        #     g_unit = Gu.by_code[gu_code][0]
        gu_hd = g_unit.hour_data[hour]
        gu_hd.changed = gu_hd.state != state
        gu_hd.state = bool(state)
        if not gu_hd.pmax_t and state:
            raise Exception('Gu %i has no pmax at hour %i' % (gu_code, hour))
        # except TypeError:
        #     print('unable to access nonexistent Gu %i!' % gu_code)

    # dependant_gus = [
    #     {'base': (300991, 300992), 'dep': 300993},
    #     {'base': (300994, 300995), 'dep': 300996},
    #     {'base': (500586, 500587), 'dep': 500588}
    # ]
    #
    # for g_unit in Gu:
    #     if not sum(_hd.changed for _hd in g_unit.hour_data) \
    #        or g_unit.code in [_['dep'] for _ in dependant_gus]:
    #         continue
    #     sum_pmax = sum(_hd.pmax * (not _hd.state if _hd.changed else _hd.state)
    #                    for _hd in g_unit.hour_data)
    #     sum_pmin = sum(_hd.pmin * (not _hd.state if _hd.changed else _hd.state)
    #                    for _hd in g_unit.hour_data)
    #     sum_state = sum(not _hd.state if _hd.changed else _hd.state
    #                     for _hd in g_unit.hour_data)
    #     avg_pmax = sum_pmax / sum_state if sum_state else sum_state
    #     avg_pmin = sum_pmin / sum_state if sum_state else sum_state
    #     for _hd in g_unit.hour_data:
    #         prev_state = not _hd.state if _hd.changed else _hd.state
    #         pmax_hour = _hd.pmax * prev_state
    #         avg_pmax_hour = avg_pmax * prev_state
    #         pmin_hour = _hd.pmin * prev_state
    #         avg_pmin_hour = avg_pmin * prev_state
    #         if abs(pmax_hour - avg_pmax_hour) > 0.2 * avg_pmax_hour:
    #             print(g_unit.code, _hd.hour, avg_pmax, abs(pmax_hour - avg_pmax_hour), 0.2 * avg_pmax_hour)
    #             _hd.pmax_t = avg_pmax_hour - pmax_hour
    #             _hd.pmin_t = avg_pmin_hour - pmin_hour
    #             _hd.delta = True
    #
    # for case in dependant_gus:
    #     base_gus_hour_data = []
    #     for code in case['base']:
    #         (g_unit,) = Gu.by_code[code]
    #         base_gus_hour_data.append(g_unit.hour_data)
    #
    #     (target,) = Gu.by_code[case['dep']]
    #
    #     for _hds in zip(*base_gus_hour_data):
    #         hour = _hds[0].hour
    #         ratio = sum(hd.state for hd in _hds) / len(_hds)
    #         target_hd = target.hour_data[hour]
    #         if not target_hd.state and target_hd.changed:
    #             target_hd.pmax_t = - target_hd.pmax
    #         else:
    #             target_hd.changed = any(_hd.changed for _hd in _hds)
    #             target_hd.state = bool(ratio)
    #             if target_hd.state:
    #                 target_hd.pmax_t = math.ceil(target_hd.pmax_t * ratio) - target_hd.pmax
    #                 target_hd.pmin_t = math.ceil(target_hd.pmin_t * ratio) - target_hd.pmin
    #                 target_hd.delta = True
    #             else:
    #                 target_hd.pmax_t = target_hd.pmax
