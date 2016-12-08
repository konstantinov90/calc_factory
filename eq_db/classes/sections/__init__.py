import settings
import time
from operator import itemgetter
from utils import DB, ORM
from utils.progress_bar import update_progress

from sql_scripts import sections_script as ss
from sql_scripts import impex_area_script as ias
from sql_scripts import line_groups_script as lgs

from .sections import Section

HOURCOUNT = 24


def make_sections(tsid={}, tdate=''):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}
    print('making sections%s' % ((' for date %s' % tdate) if tdate else ''))
    start_time = time.time()

    con = DB.OracleConnection()

    @DB.process_cursor(con, ss, tsid)
    def process_sections(new_row, sections_list):
        section_code = new_row[ss['code']]
        section = sections_list[section_code]
        if not section:
            section = sections_list.add_section(new_row)
        section.add_section_hour_data(new_row)

    @DB.process_cursor(con, lgs, tsid)
    def process_line_groups(new_row, sections_list):
        section_code = new_row[lgs['section_code']]
        section = sections_list[section_code]
        if section:
            section.add_section_hour_line_data(new_row)

    # print('loading sections information')
    
    sections = SectionsList()
    sections.set_impex_area_data(con.exec_script(ias.get_query(), tsid))
    process_sections(sections)
    process_line_groups(sections)

    for i, section in enumerate(sections):
        section.serialize(ORM.session)
        update_progress((i + 1) / len(sections))

    ORM.session.commit()

    print('%s %i seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))

    return sections


class SectionsList(object):
    def __init__(self):
        self.sections_list = []
        self.sections_list_index = {}
        self.sections_lines_data = [[] for hour in range(HOURCOUNT)]
        self.sections_data = [[] for hour in range(HOURCOUNT)]
        self.sections_lines_impex_data = [[] for hour in range(HOURCOUNT)]
        self.sections_impex_data = [[] for hour in range(HOURCOUNT)]
        self.prepared_impex_area_data = []

    def __iter__(self):
        for l in self.sections_list:
            yield l

    def __len__(self):
        return len(self.sections_list)

    def __getitem__(self, item):
        if item in self.sections_list_index.keys():
            return self.sections_list[self.sections_list_index[item]]
        else:
            return None

    def add_section(self, ss_row):
        section_code = ss_row[ss['code']]
        self.sections_list_index[section_code] = len(self.sections_list)
        new_section = Section(ss_row)
        self.sections_list.append(new_section)
        return new_section

    def set_impex_area_data(self, impex_area_data):
        Section.impex_area_data = impex_area_data

    def prepare_sections_data(self):
        s_s = {'hour': 0, 'code': 0}
        h_int = s_s['hour']
        for section in self.sections_list:
            [self.sections_data[d[h_int]].append(d[:h_int] + d[(h_int + 1):])
                for d in section.get_section_data()]
        [l.sort(key=itemgetter(s_s['code'])) for l in self.sections_data]

    def prepare_sections_lines_data(self):
        s_s = {'hour': 0, 'node_from_code': 1, 'node_to_code': 2, 'parallel_num': 0}
        h_int = s_s['hour']
        for section in self.sections_list:
            [self.sections_lines_data[d[h_int]].append(d[:h_int] + d[(h_int + 1):])
                for d in section.get_section_lines_data()]
        [l.sort(key=itemgetter(s_s['node_from_code'], s_s['node_to_code'], s_s['parallel_num'])) for l in self.sections_lines_data]

    def prepare_sections_impex_data(self):
        s_s = {'hour': 0, 'code': 0}
        h_int = s_s['hour']
        for section in self.sections_list:
            [self.sections_impex_data[d[h_int]].append(d[:h_int] + d[(h_int + 1):])
                for d in section.get_section_impex_data()]
        [l.sort(key=itemgetter(s_s['code'])) for l in self.sections_impex_data]

    def prepare_sections_lines_impex_data(self):
        s_s = {'hour': 0, 'node_from_code': 1, 'node_to_code': 2, 'parallel_num': 0}
        h_int = s_s['hour']
        for section in self.sections_list:
            [self.sections_lines_impex_data[d[h_int]].append(d[:h_int] + d[(h_int + 1):])
                for d in section.get_section_lines_impex_data()]
        [l.sort(key=itemgetter(s_s['node_from_code'], s_s['node_to_code'], s_s['parallel_num'])) for l in self.sections_lines_impex_data]

    def get_prepared_sections_data(self):
        if not self.sections_data[0]:
            self.prepare_sections_data()
        return self.sections_data

    def get_prepared_sections_lines_data(self):
        if not self.sections_lines_data[0]:
            self.prepare_sections_lines_data()
        return self.sections_lines_data

    def get_prepared_sections_impex_data(self):
        if not self.sections_impex_data[0]:
            self.prepare_sections_impex_data()
        return self.sections_impex_data

    def get_prepared_sections_lines_impex_data(self):
        if not self.sections_lines_impex_data[0]:
            self.prepare_sections_lines_impex_data()
        return self.sections_lines_impex_data

    def get_prepared_impex_area_data(self):
        if not self.prepared_impex_area_data:
            self.prepare_impex_area_data()
        return self.prepared_impex_area_data

    def prepare_impex_area_data(self):
        for row in Section.impex_area_data:
            section_code = row[ias['number']]
            if section_code:
                is_europe = row[ias['is_europe']]
                if section_code == settings.section_nkaz:
                    price_zone = 0
                elif is_europe == 1:
                    price_zone = 1
                elif is_europe == 0:
                    price_zone = 2
                area = row[ias['area']]
                self.prepared_impex_area_data.append ((
                    float(section_code), area if area else 0, price_zone
                ))
        self.prepared_impex_area_data = sorted(self.prepared_impex_area_data, key=itemgetter(2, 0, 1))
