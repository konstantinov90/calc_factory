import time
import re
import traceback
from utils import DB, ORM
from utils.progress_bar import update_progress
from sql_scripts import gus_script as gs
from sql_scripts import nblock_script as ns
from .gus import Gu
from .gus_hour_data import GuHourData

INRIO = True
NOTINRIO = False

def make_gus(tsid={}, tdate=''):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}
    print('making GUs%s' % ((' for date %s' % tdate) if tdate else ''))
    start_time = time.time()

    con = DB.OracleConnection()

    gus = GUsList()
    gus_hour = []

    @DB.process_cursor(con, gs, tsid)
    def process_gus(new_row, gus_list):
        gus_list.add_gu_from_rio(new_row)

    @DB.process_cursor(con, ns, tsid)
    def process_nblock(new_row, gus_list):
        gu_code = new_row[ns['gu_code']]
        # gus = gus_list[gu_code]
        if not gus_list[gu_code]:
            gus_list.add_gu_from_nblock(new_row)
        # for gu in gus_list[gu_code]:
            # gu.add_gu_hour_data(new_row)
        gus_hour.append(GuHourData(new_row))


    process_gus(gus)
    process_nblock(gus)
    ORM.session.add_all(gus_hour)
    for i, gu in enumerate(gus):
        # print(gu)
        ORM.session.add_all(gu)
        # ORM.session.add_all(gu.gu_hour_data.values())
        update_progress((i + 1) / len(gus))

    ORM.session.commit()
    print('%s %s seconds %s' % (15 * '-',  round(time.time() - start_time, 3), 15 * '-'))

    return gus


class GUsList(object):
    def __init__(self):
        self.gus_list = {}
        # self.gus_list_index = {}

    def __len__(self):
        return len(self.gus_list)

    def __iter__(self):
        for g in self.gus_list.values():
            yield g

    def __getitem__(self, item):
        if item in self.gus_list.keys():
            return self.gus_list[item]
        else:
            return None

    def add_gu_from_rio(self, gs_row):
        gu_code = gs_row[gs['code']]
        if gu_code not in self.gus_list.keys():
            self.gus_list[gu_code] = []
        self.gus_list[gu_code].append(Gu(gs_row, INRIO))

    def add_gu_from_nblock(self, ns_row):
        gu_code = ns_row[ns['gu_code']]
        self.gus_list[gu_code] = [Gu(ns_row, NOTINRIO)]
        print('WARNING!! added GU %i not from RIO!' % gu_code)

    def set_parent_dgus(self, dgus_list):
        for gus in self.gus_list.values():
            for gu in gus:
                gu.set_parent_dgu(dgus_list)

    def set_to_remove(self, gu_code):
        if gu_code in self.gus_list.keys():
            for gu in self.gus_list[gu_code]:
                gu.set_to_remove()
        else:
            # print('ERROR! tried to set to remove nonexistent GU #%i' % gu_code)
            try:
                raise Exception('ERROR! tried to set to remove nonexistent GU #%i' % gu_code)
            except Exception:
                traceback.print_exc()

    def remove_gu(self, gu_code):
        if gu_code in self.gus_list.keys():
            L = len(self.gus_list[gu_code])
            del self.gus_list[gu_code]
            return L
        else:
            return False
