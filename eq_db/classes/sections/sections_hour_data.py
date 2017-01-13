"""Class SectionHourData."""
import itertools
from .sections_hour_line_data import SectionHourLineData


class SectionHourData(object):
    """class SectionHourData"""
    seq = itertools.count()

    def __init__(self, ss_row):
        self._id = next(self.seq)
        self.hour, self.section_code, self.p_max, self.p_min, *_ = ss_row
        self.state = True if ss_row.state else False
        self.line_data = []
        # self.max_price = None
        # self.p_max, self.p_min = 9999, -9999

    def add_section_line(self, lgs_row):
        """add line_data to instance"""
        self.line_data.append(SectionHourLineData(lgs_row, self._id))

    # def set_max_price(self, max_price):
    #     """set hour max bid price"""
    #     self.max_price = max_price

    def attach_lines(self, lines_list):
        """attach lines to instance's line_data"""
        for _line_data in self.line_data:
            _line_data.attach_lines(lines_list)

    def __repr__(self):
        return '<SectionHourData> %i: %s' % (self.section_code, 'ON' if self.state else 'OFF')
