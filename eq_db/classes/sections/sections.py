"""Class Section."""
from operator import attrgetter
from ..meta_base import MetaBase
from .sections_hour_data import SectionHourData

HOURCOUNT = 24


class Section(object, metaclass=MetaBase):
    """class Section"""
    def __init__(self, ss_row):
        self.code = ss_row.code
        self.type = ss_row.type
        self.hours = {}
        self.dpgs = []
        self.impex_data = []
        self._init_on_load()

    @property
    def is_optimizable(self):
        """is_optimizable property"""
        return sum(map(attrgetter('is_optimizable'), self.impex_data)) > 0

    @property
    def hour_data(self):
        """hour_data property"""
        return sorted(self.hours.values(), key=attrgetter('hour'))

    def __repr__(self):
        return '<Section %i>' % self.code

    def add_dpg(self, dpg):
        """add Dpg intance to dpgs list"""
        self.dpgs.append(dpg)

    def set_impex_data(self, impex_areas_list):
        """set impex data, if any"""
        self.impex_data = [ia for ia in impex_areas_list if ia.section_code == self.code]

    # def set_max_price(self, max_price_list):
    #     """pass max bid prices to hour data"""
    #     for _hd in self.hours.values():
    #         _hd.set_max_price(max_price_list[_hd.hour])

    def attach_lines(self, lines_list):
        """pass lines to hour data"""
        for _hd in self.hours.values():
            _hd.attach_lines(lines_list)

    def _init_on_load(self):
        """additional initialization"""
        if self.code not in self.lst.keys():
            self.lst[self.code] = self
        self.section_impex_data = []
        self.section_lines_impex_data = []
        self.section_data = []
        self.section_lines_data = []

    def add_section_hour_data(self, ss_row):
        """add SectionHourData instance"""
        self._check_hour_data(ss_row)
        hour = ss_row.hour
        if hour in self.hours.keys():
            raise Exception('tried to add same section %i hour twice' % self.code)
        self.hours[hour] = SectionHourData(ss_row)

    def add_section_hour_line_data(self, lgs_row):  # def attach_line (self, lgs_row, lines_list):
        """pass lines group data to SectionHourData instance"""
        hour = lgs_row.hour
        self.hours[hour].add_section_line(lgs_row)

    def _check_hour_data(self, ss_row):
        """check hour data consistency"""
        for _datum in ['code', 'type']:
            self._check_datum(_datum, getattr(ss_row, _datum))

    def _check_datum(self, attr, datum):
        """check single property consistency"""
        if getattr(self, attr) != datum:
            raise Exception('%s not consistent for section %i' % (attr, self.code))

    def get_section_impex_data(self):
        """get eq_db_sections_impex view data"""
        if not self.section_impex_data:
            self.prepare_section_impex_data()
        return self.section_impex_data

    def prepare_section_impex_data(self):
        """prepare eq_db_sections_impex view data"""
        if (not self.dpgs) or (not self.type):
            # если нет ГТП или тип равен 0 - то это не импорт/экспорт
            return
        for _hd in self.hour_data:
            for _ld in _hd.line_data:
                if _ld.skip:
                    continue
                for _line in _ld.lines:
                    if _line.get_line_hour_state(_hd.hour):
                        # если есть хоть одна включенная линия - добавляем запись
                        self.section_impex_data.append((
                            _hd.hour, self.code, 0.
                        ))
                        break  # но не более одной записи
                else:
                    continue
                break

    def get_section_lines_impex_data(self):
        """get eq_db_section_impex_lines view data"""
        if not self.section_lines_impex_data:
            self.prepare_section_lines_impex_data()
        return self.section_lines_impex_data

    def prepare_section_lines_impex_data(self):
        """prepare eq_db_section_impex_lines view data"""
        if (not self.dpgs) or (not self.type):
            # если нет ГТП или тип равен 0 - то это не импорт/экспорт
            return
        for _hd in self.hour_data:
            for _ld in _hd.line_data:
                if _ld.skip:
                    continue
                for _line in _ld.lines:
                    if _line.get_line_hour_state(_hd.hour):
                        self.section_lines_impex_data.append((
                            _hd.hour, _line.parallel_num, _ld.node_from_code, _ld.node_to_code,
                            _ld.div, self.code
                        ))

    def get_section_data(self):
        """get eq_db_sections view data"""
        if not self.section_data:
            self.prepare_section_data()
        return self.section_data

    def prepare_section_data(self):
        """prepare eq_db_sections view data"""
        for _hd in self.hour_data:
            if not _hd.state:  # если state равен 0 - то это неконтролируемое сечение
                continue
            for _ld in _hd.line_data:
                for _line in _ld.lines:
                    if _line.get_line_hour_state(_hd.hour):
                        if self.is_optimizable:
                            p_max = 9999
                            p_min = -9999
                        else:
                            p_max = _hd.p_max
                            p_min = _hd.p_min

                        self.section_data.append((
                            _hd.hour, self.code, p_max, p_min
                        ))
                        break
                else:
                    continue
                break

    def get_section_lines_data(self):
        """get eq_db_section_lines view data"""
        if not self.section_lines_data:
            self.prepare_section_lines_data()
        return self.section_lines_data

    def prepare_section_lines_data(self):
        """prepare eq_db_section_lines view data"""
        for _hd in self.hour_data:
            if not _hd.state:  # если state равен 0 - то это неконтролируемое сечение
                continue
            for _ld in _hd.line_data:
                if _ld.skip:
                    continue
                for _line in _ld.lines:
                    if _line.get_line_hour_state(_hd.hour):
                        self.section_lines_data.append((
                            _hd.hour, _line.parallel_num, _ld.node_from_code, _ld.node_to_code,
                            _ld.div, self.code
                        ))
