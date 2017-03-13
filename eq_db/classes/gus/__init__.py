"""Create Gu instances."""
import csv
import math
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
        for gu in Gu:
            if fuel_type in gu.fuel_type_list:
                gu.bid_factor = factor

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
            [_gu] = Gu.by_code[gu_code]
        except TypeError:
            print('changed Gu %i is blocked!' % gu_code)
            raise
        #     #     raise
        #     _gu = Gu.by_code[gu_code][0]
        gu_hd = _gu.hour_data[hour]
        gu_hd.changed = gu_hd.state != state
        gu_hd.state = state
        if not gu_hd.pmax_t and state:
            raise Exception('Gu %i has no pmax at hour %i' % (gu_code, hour))
        # except TypeError:
        #     print('unable to access nonexistent Gu %i!' % gu_code)


    dependant_gus = [
        {'base': (300991, 300992), 'dep': 300993},
        {'base': (300994, 300995), 'dep': 300996}]

    for case in dependant_gus:
        base_gus_hour_data = []
        for code in case['base']:
            (gu,) = Gu.by_code[code]
            base_gus_hour_data.append(gu.hour_data)

        (target,) = Gu.by_code[case['dep']]

        for _hds in zip(*base_gus_hour_data):
            hour = _hds[0].hour
            ratio = sum(_hd.state for _hd in _hds) / len(_hds)
            target_hd = target.hour_data[hour]
            target_hd.changed = any(_hd.changed for _hd in _hds)
            target_hd.state = bool(ratio)
            if target_hd.state:
                target_hd.pmax_t = math.ceil(target_hd.pmax_t * ratio) - target_hd.pmax
            else:
                target_hd.pmax_t = target_hd.pmax
