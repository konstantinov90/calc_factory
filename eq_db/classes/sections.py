import settings
import time
from operator import itemgetter
from utils import DB
from utils.progress_bar import update_progress
from eq_db.classes.lines import make_lines
from sql_scripts import SectionsScript
from sql_scripts import ImpexAreaScript
from sql_scripts import LineGroupsScript


HOURCOUNT = 24

ss = SectionsScript()
ias = ImpexAreaScript()
lgs = LineGroupsScript()


def make_sections(tsid={}, tdate='', line_list=[]):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}
    if not line_list:
        line_list = make_lines(tsid, tdate)
    print('making sections%s' % ((' for date %s' % tdate) if tdate else ''))
    start_time = time.time()

    con = DB.OracleConnection()

    sections = SectionsList()
    sections.set_impex_area_data(con.exec_script(ias.get_query(), tsid))

    @DB.process_cursor(con, ss, tsid)
    def process_sections(new_row, sections_list):
        section_code = new_row[ss['section_code']]
        section = sections_list[section_code]
        if not section:
            section = sections_list.add_section(new_row)
        section.add_section_hour_data(new_row)

    @DB.process_cursor(con, lgs, tsid)
    def process_line_groups(new_row, sections_list, lines_list):
        section_code = new_row[lgs['section_code']]
        section = sections_list[section_code]
        if section:
            section.attach_line(new_row, lines_list)

    # print('loading sections information')
    process_sections(sections)
    process_line_groups(sections, line_list)

    # for i, section in enumerate(sections):
    #     section.prepare_data()
    #     update_progress((i + 1) / len(sections))

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
        section_code = ss_row[ss['section_code']]
        self.sections_list_index[section_code] = len(self.sections_list)
        new_section = Section(ss_row)
        self.sections_list.append(new_section)
        return new_section

    def set_impex_area_data(self, impex_area_data):
        Section.impex_area_data = impex_area_data

    def prepare_sections_data(self):
        s_s = {'hour': 0, 'section_code': 0}
        h_int = s_s['hour']
        for section in self.sections_list:
            [self.sections_data[d[h_int]].append(d[:h_int] + d[(h_int + 1):])
                for d in section.get_section_data()]
        [l.sort(key=itemgetter(s_s['section_code'])) for l in self.sections_data]

    def prepare_sections_lines_data(self):
        s_s = {'hour': 0, 'node_from_code': 1, 'node_to_code': 2, 'parallel_num': 0}
        h_int = s_s['hour']
        for section in self.sections_list:
            [self.sections_lines_data[d[h_int]].append(d[:h_int] + d[(h_int + 1):])
                for d in section.get_section_lines_data()]
        [l.sort(key=itemgetter(s_s['node_from_code'], s_s['node_to_code'], s_s['parallel_num'])) for l in self.sections_lines_data]

    def prepare_sections_impex_data(self):
        s_s = {'hour': 0, 'section_code': 0}
        h_int = s_s['hour']
        for section in self.sections_list:
            [self.sections_impex_data[d[h_int]].append(d[:h_int] + d[(h_int + 1):])
                for d in section.get_section_impex_data()]
        [l.sort(key=itemgetter(s_s['section_code'])) for l in self.sections_impex_data]

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
            section_code = row[ias['section_number']]
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



class SectionHourData(object):
    def __init__(self, ss_row):
        self.p_max = ss_row[ss['p_max']]
        self.p_min = ss_row[ss['p_min']]
        self.state = ss_row[ss['state']]


class Section(object):
    def __init__(self, ss_row):
        self.section_code = ss_row[ss['section_code']]
        self.type = ss_row[ss['type']]
        self.hour_data = [0] * HOURCOUNT
        self.dpgs = []
        self.lines = []
        self.section_impex_data = []
        self.section_lines_impex_data = []
        self.section_data = []
        self.section_lines_data = []
        self.skip_lines = [[] for hour in range(HOURCOUNT)]

    def add_section_hour_data(self, ss_row):
        self._check_hour_data(ss_row)
        hour = ss_row[ss['hour']]
        self.hour_data[hour] = SectionHourData(ss_row)

    def add_dpg(self, dpg):
        self.dpgs.append(dpg)

    def attach_line(self, lgs_row, lines_list):
        # если состояние == 1 то линию присоединять не надо
        skip = [True if lgs_row[lgs['state']] else False]
        hour = lgs_row[lgs['hour']]
        node_from_code = lgs_row[lgs['node_from_code']]
        node_to_code = lgs_row[lgs['node_to_code']]
        div = lgs_row[lgs['div']]
        lines = lines_list.get_line(node_from_code, node_to_code)
        flipped = False
        if not lines:
            lines = lines_list.get_line(node_to_code, node_from_code)
            flipped = True
        if lines:
            for line in lines:
                if lgs_row[lgs['state']]:
                    self.skip_lines[hour].append(line)
                if line.get_line_hour_state(hour) and (line not in self.lines):
                    line.set_group_line_div(self.section_code, div)
                    line.set_group_flipped(self.section_code, flipped)
                    self.lines.append(line)

    def is_optimizable(self):
        return sum([i[ias['optimized']] for i in self.impex_area_data
                    if i[ias['section_number']] == self.section_code]) > 0

    def _check_hour_data(self, ss_row):
        for d in ['section_code', 'type']:
            self._check_datum(d, ss_row[ss[d]])

    def _check_datum(self, attr, datum):
        if getattr(self, attr) != datum:
            raise Exception('%s not consistent for section %i' % (attr, self.section_code))

    def get_section_impex_data(self):
        if not self.section_impex_data:
            self.prepare_section_impex_data()
        return self.section_impex_data

    def prepare_section_impex_data(self):
        if (not self.dpgs) or (not self.type):  # если нет ГТП или тип равен 0 - то это не импорт/экспорт
            return
        for hour, sh in enumerate(self.hour_data):
            for l in self.lines:
                if l.get_line_hour_state(hour):  # если есть хоть одна включенная линия - добавляем запись
                    self.section_impex_data.append((
                        hour, self.section_code, 0.
                    ))
                    break  # но не более одной записи

    def get_section_lines_impex_data(self):
        if not self.section_lines_impex_data:
            self.prepare_section_lines_impex_data()
        return self.section_lines_impex_data

    def prepare_section_lines_impex_data(self):
        if (not self.dpgs) or (not self.type):  # если нет ГТП или тип равен 0 - то это не импорт/экспорт
            return
        for hour, sh in enumerate(self.hour_data):
            for l in self.lines:
                if l.get_line_hour_state(hour):
                    if l.group_line_flipped[self.section_code] and not l in self.skip_lines[hour]:
                        node_from_code = l.node_to_code
                        node_to_code = l.node_from_code
                    else:
                        node_from_code = l.node_from_code
                        node_to_code = l.node_to_code
                    self.section_lines_impex_data.append((
                        hour, l.parallel_num, node_from_code, node_to_code,
                        float(l.group_line_div[self.section_code]), self.section_code
                    ))

    def get_section_data(self):
        if not self.section_data:
            self.prepare_section_data()
        return self.section_data

    def prepare_section_data(self):
        for hour, sh in enumerate(self.hour_data):
            if self.hour_data[hour].state:  # если state равен 0 - то это неконтролируемое сечение
                for l in self.lines:
                    if l.get_line_hour_state(hour):
                        if self.is_optimizable():
                            p_max = 9999
                            p_min = -9999
                        else:
                            p_max = self.hour_data[hour].p_max
                            p_min = self.hour_data[hour].p_min

                        self.section_data.append((
                            hour, self.section_code, p_max, p_min
                        ))
                        break

    def get_section_lines_data(self):
        if not self.section_lines_data:
            self.prepare_section_lines_data()
        return self.section_lines_data

    def prepare_section_lines_data(self):
        for hour, sh in enumerate(self.hour_data):
            if self.hour_data[hour].state:  # если state равен 0 - то это неконтролируемое сечение
                for l in self.lines:
                    if l.get_line_hour_state(hour) and not l in self.skip_lines[hour]:
                        if l.group_line_flipped[self.section_code]:
                            node_from_code = l.node_to_code
                            node_to_code = l.node_from_code
                        else:
                            node_from_code = l.node_from_code
                            node_to_code = l.node_to_code
                        self.section_lines_data.append((
                            hour, l.parallel_num, node_from_code, node_to_code,
                            l.group_line_div[self.section_code], self.section_code
                        ))
