import time
import re
import traceback
from utils import DB
from sql_scripts import GUsScript
from sql_scripts import NBlockScript

gs = GUsScript()
ns = NBlockScript()

INRIO = True
NOTINRIO = False

def make_gus(tsid={}, tdate=''):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}
    print('making GUs%s' % ((' for date %s' % tdate) if tdate else ''))
    start_time = time.time()

    con = DB.OracleConnection()

    gus = GUsList()

    @DB.process_cursor(con, gs, tsid)
    def process_gus(new_row, gus_list):
        gus_list.add_gu_from_rio(new_row)

    @DB.process_cursor(con, ns, tsid)
    def process_nblock(new_row, gus_list):
        gu_code = new_row[ns['gu_code']]
        # gus = gus_list[gu_code]
        if not gus_list[gu_code]:
            gus_list.add_gu_from_nblock(new_row)
        for gu in gus_list[gu_code]:
            gu.add_gu_hour_data(new_row)


    process_gus(gus)
    process_nblock(gus)

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
        self.gus_list[gu_code].append(GU(gs_row, INRIO))

    def add_gu_from_nblock(self, ns_row):
        gu_code = ns_row[ns['gu_code']]
        self.gus_list[gu_code] = [GU(ns_row, NOTINRIO)]
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


class GUHourData(object):
    def __init__(self, ns_row):
        self.pmin = ns_row[ns['pmin']]
        self.pmax = ns_row[ns['pmax']]
        self.pmax_t = ns_row[ns['pmax_t']]
        self.pmin_t = ns_row[ns['pmin_t']]
        self.state = ns_row[ns['state']]
        self.repair = ns_row[ns['repair']]
        self.is_sysgen = ns_row[ns['is_sysgen']]
        self.vgain = ns_row[ns['vgain']]
        self.vdrop = ns_row[ns['vdrop']]


class GU(object):
    def __init__(self, row, is_in_rio):
        self.is_in_rio = is_in_rio
        self.gu_hour_data = {}
        self.dgu = None
        self.is_remove = False
        if self.is_in_rio:
            gs_row = row
            self.code = gs_row[gs['code']]
            self.dgu_id = gs_row[gs['dgu_id']]
            ftl = gs_row[gs['fuel_type_list']]
            self.fuel_type_list = list(map(int, re.split(',', ftl))) if ftl else []
            self.fixed_power = gs_row[gs['fixed_power']]
            self.rge_code = None
        else:
            ns_row = row
            self.code = ns_row[ns['gu_code']]
            self.dgu_id = None
            self.fuel_type_list = []
            self.fixed_power = None
            self.rge_code = ns_row[ns['dgu_code']]

    def set_to_remove(self):
        self.is_remove = True

    def set_parent_dgu(self, dgus_list):
        if self.is_in_rio:
            dgu = dgus_list.get_dgu_by_id(self.dgu_id)
        else:
            dgu = dgus_list.get_dgu_by_code(self.rge_code)
            self.dgu_id = dgu.id
        if self.rge_code and dgu.code != self.rge_code:
            # raise Exception('GU %i trader RGE code not consistent with rastr rge_Code!' % self.code)
            print('GU %i trader RGE code not consistent with rastr rge_Code!' % self.code)
            self.gu_hour_data = {}
        self.dgu = dgu
        try:
            dgu.add_gu(self)
        except Exception:
            print(self.rge_code, self.code, self.dgu_id)
            raise Exception('GU!')

    def add_gu_hour_data(self, ns_row):
        rge_code = ns_row[ns['dgu_code']]
        if not self.rge_code:
            self.rge_code = rge_code
        if self.rge_code != rge_code:
            raise Exception('rge_code for GU %i not consistent!' % self.code)
        self.gu_hour_data[ ns_row[ns['hour']] ] = GUHourData(ns_row)
