"""Create Gu instances."""
import csv
from utils import DB
# from utils.printer import print
from utils.trade_session_manager import ts_manager
from sql_scripts import gus_script as gs, nblock_script as ns, gus_script_v as gs_v, \
nblock_script_v as ns_v, block_out_v as bo_v
from .gus import Gu
from .gus_hour_data import GuHourData


@ts_manager
def make_gus(tsid):
    """create Gu instances"""
    con = DB.OracleConnection()
    Gu.clear()

    for new_row in con.script_cursor(gs, tsid=tsid):
        Gu(new_row)

    for new_row in con.script_cursor(ns, tsid=tsid):
        gu_code = new_row.gu_code
        if not Gu.by_code[gu_code]:
            Gu.from_nblock(new_row)
            print('WARNING!! added Gu %i not from RIO!' % gu_code)
        unit_hd = GuHourData(new_row)
        for unit in Gu.by_code[gu_code]:
            unit.add_gu_hour_data(new_row, unit_hd)

def _blocks_reader(scenario):
    with open(r'vsvgo\blocks_scenario_%i.csv' % scenario, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        for row in reader:
            gu_code, hour, state = (int(i) for i in row)
            state = bool(state)
            yield gu_code, hour, state

@ts_manager
def add_gus_vertica(scenario):
    """add Gus instances from Vertica DB"""
    con = DB.VerticaConnection()
    for new_row in con.script_cursor(gs_v, scenario=scenario):
        Gu(new_row)

    for new_row in con.script_cursor(ns_v, scenario=scenario):
        Gu.by_id[new_row.gu_code].add_gu_hour_data(new_row, GuHourData(new_row))

    # load turned off blocks from vertica
    for gu_code, *_ in con.script_cursor(bo_v, scenario=scenario):
        try:
            for _gu in Gu.by_code[gu_code]:
                for gu_hd in _gu.hour_data:
                    gu_hd.changed = gu_hd.state
                    gu_hd.state = False
        except TypeError:
            print('Attempted to access nonexistent Gu %i!' % gu_code)

    # get new blocks' states from vsvgo
    # for gu_code, hour, state in _blocks_reader(scenario):
    #     # try:
    #     [_gu] = Gu.by_code[gu_code]
    #     # except ValueError:
    #     #     print('changed Gu %i is blocked!' % gu_code)
    #     #     #     raise
    #     #     _gu = Gu.by_code[gu_code][0]
    #     gu_hd = _gu.hour_data[hour]
    #     gu_hd.changed = gu_hd.state != state
    #     gu_hd.state = state
    #     if not gu_hd.pmax_t and state:
    #         raise Exception('Gu %i has no pmax at hour %i' % (gu_code, hour))
    #     # except TypeError:
    #     #     print('unable to access nonexistent Gu %i!' % gu_code)
