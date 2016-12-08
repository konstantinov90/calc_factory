from sqlalchemy import *
from sqlalchemy.orm import relationship, reconstructor

from utils.ORM import Base, MetaBase

from sql_scripts import sections_script as ss
from sql_scripts import line_groups_script as lgs
from sql_scripts import impex_area_script as ias

from .sections_hour_data import SectionHourData

HOURCOUNT = 24


class Section(Base, metaclass=MetaBase):
    __tablename__ = 'sections'
    code = Column(Integer, primary_key=True)
    type = Column(Integer)
    is_optimizable = Column(Boolean)

    hour_data = relationship('SectionHourData', order_by='SectionHourData.hour')
    dpgs = relationship('DpgImpex', primaryjoin='DpgImpex.section_code == Section.code', foreign_keys='DpgImpex.section_code')

    def __init__(self, ss_row):
        self.code = ss_row[ss['code']]
        self.type = ss_row[ss['type']]
        self.is_optimizable = sum([i[ias['optimized']] for i in self.impex_area_data
                                if i[ias['section_number']] == self.code]) > 0
        self.hours = {}
        self._init_on_load()
        # self.dpgs = []
        # self.lines = []
        # self.skip_lines = [[] for hour in range(HOURCOUNT)]

    @reconstructor
    def _init_on_load(self):
        if self.code not in self.lst.keys():
            self.lst[self.code] = self
        self.section_impex_data = []
        self.section_lines_impex_data = []
        self.section_data = []
        self.section_lines_data = []

    def add_section_hour_data(self, ss_row):
        self._check_hour_data(ss_row)
        hour = ss_row[ss['hour']]
        if hour in self.hours.keys():
            raise Exception('tried to add same section %i hour twice' % self.code)
        self.hours[hour] = SectionHourData(ss_row)

    def add_dpg(self, dpg):
        self.dpgs.append(dpg)

    def add_section_hour_line_data(self, lgs_row):  # def attach_line (self, lgs_row, lines_list):
        hour = lgs_row[lgs['hour']]
        self.hours[hour].add_section_line(lgs_row)

        # если состояние == 1 то линию присоединять не надо
        # skip = [True if lgs_row[lgs['state']] else False]
        # hour = lgs_row[lgs['hour']]
        # node_from_code = lgs_row[lgs['node_from_code']]
        # node_to_code = lgs_row[lgs['node_to_code']]
        # div = lgs_row[lgs['div']]
        # lines = lines_list.get_line(node_from_code, node_to_code)
        # flipped = False
        # if not lines:
        #     lines = lines_list.get_line(node_to_code, node_from_code)
        #     flipped = True
        # if lines:
        #     for line in lines:
        #         if lgs_row[lgs['state']]:
        #             self.skip_lines[hour].append(line)
        #         if line.get_line_hour_state(hour) and (line not in self.lines):
        #             line.set_group_line_div(self.section_code, div)
        #             line.set_group_flipped(self.section_code, flipped)
        #             self.lines.append(line)

    # def is_optimizable(self):
    #     return sum([i[ias['optimized']] for i in self.impex_area_data
    #                 if i[ias['section_number']] == self.code]) > 0

    def _check_hour_data(self, ss_row):
        for d in ['code', 'type']:
            self._check_datum(d, ss_row[ss[d]])

    def _check_datum(self, attr, datum):
        if getattr(self, attr) != datum:
            raise Exception('%s not consistent for section %i' % (attr, self.code))

    def get_section_impex_data(self):
        if not self.section_impex_data:
            self.prepare_section_impex_data()
        return self.section_impex_data

    def prepare_section_impex_data(self):
        if (not self.dpgs) or (not self.type):  # если нет ГТП или тип равен 0 - то это не импорт/экспорт
            return
        for hd in self.hour_data:
            for ld in hd.line_data:
                if ld.skip:
                    continue
                for l in ld.lines:
                    if l.get_line_hour_state(hd.hour):  # если есть хоть одна включенная линия - добавляем запись
                        self.section_impex_data.append((
                            hd.hour, self.code, 0.
                        ))
                        break  # но не более одной записи
                else:
                    continue
                break

    def get_section_lines_impex_data(self):
        if not self.section_lines_impex_data:
            self.prepare_section_lines_impex_data()
        return self.section_lines_impex_data

    def prepare_section_lines_impex_data(self):
        if (not self.dpgs) or (not self.type):  # если нет ГТП или тип равен 0 - то это не импорт/экспорт
            return
        for hd in self.hour_data:
            for ld in hd.line_data:
                if ld.skip:
                    continue
                for l in ld.lines:
                    if l.get_line_hour_state(hd.hour):
                        self.section_lines_impex_data.append((
                            hd.hour, l.parallel_num, ld.node_from_code, ld.node_to_code,
                            ld.div, self.code
                        ))

    def get_section_data(self):
        if not self.section_data:
            self.prepare_section_data()
        return self.section_data

    def prepare_section_data(self):
        for hd in self.hour_data:
            if not hd.state:  # если state равен 0 - то это неконтролируемое сечение
                continue
            for ld in hd.line_data:
                for l in ld.lines:
                    if l.get_line_hour_state(hd.hour):
                        if self.is_optimizable:
                            p_max = 9999
                            p_min = -9999
                        else:
                            p_max = hd.p_max
                            p_min = hd.p_min

                        self.section_data.append((
                            hd.hour, self.code, p_max, p_min
                        ))
                        break
                else:
                    continue
                break

    def get_section_lines_data(self):
        if not self.section_lines_data:
            self.prepare_section_lines_data()
        return self.section_lines_data

    def prepare_section_lines_data(self):
        for hd in self.hour_data:
            if not hd.state:  # если state равен 0 - то это неконтролируемое сечение
                continue
            for ld in hd.line_data:
                if ld.skip:
                    continue
                for l in ld.lines:
                    if l.get_line_hour_state(hd.hour):
                        self.section_lines_data.append((
                            hd.hour, l.parallel_num, ld.node_from_code, ld.node_to_code,
                            ld.div, self.code
                        ))

    def serialize(self, session):
        session.add(self)
        session.flush()
        session.add_all(self.hours.values())
        session.flush()
        for hd in self.hours.values():
            session.add_all(hd.lines)
