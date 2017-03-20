"""Class Line."""
import itertools
import math
from operator import attrgetter
from utils.subscriptable import subscriptable
from ..meta_base import MetaBase
from .lines_hour_data import LineHourData


class Line(object, metaclass=MetaBase):
    """class Line"""
    # seq = itertools.count()

    def __init__(self, ls_row, is_new=False):
        # self._id = next(self.seq)
        self.node_from_code, self.node_to_code, self.parallel_num, self.kt_re, \
            self.kt_im, self.div, self.type, self.area_code, *_ = ls_row
        self.is_new = is_new
        self.is_turned_off = False
        self._hour_data = {}
        self.node_from = None
        self.node_to = None
        self.eq_db_lines_data = []
        self._init_on_load()

    @property
    def hour_data(self):
        """hour_data property"""
        return sorted(self._hour_data.values(), key=attrgetter('hour'))

    def set_nodes(self, nodes_list):
        """connect nodes to Line instance"""
        try:
            self.node_from = nodes_list[self.node_from_code]
            self.node_from.add_line(self)
            self.node_to = nodes_list[self.node_to_code]
            self.node_to.add_line(self)
        except AttributeError:
            raise Exception('line %r has no node(s)!' % self)

    # lines_index = {}
    lst = {'key': {}, 'nodes': {}}
    key = 'key'
    def _init_on_load(self):
        """additional initialization (complex Line index in particular)"""
        nfc = self.node_from_code
        ntc = self.node_to_code
        par_num = self.parallel_num
        idx = (nfc, ntc, par_num)
        if not idx in self.lst['key'].keys():
            self.lst['key'][idx] = self
            self.lst['nodes'].setdefault((nfc, ntc), []).append(self)
            # self.lst.setdefault((ntc, nfc), []).append(self)
            # self.lst.setdefault(nfc, []).append(self)
            # self.lst.setdefault(ntc, []).append(self)
        else:
            raise Exception('tried to add same line %i - %i n_par = %i twice!'
                            % idx)

        # if nfc in self.lines_index.keys():
        #     if ntc in self.lines_index[nfc].keys():
        #         if par_num in self.lines_index[nfc][ntc].keys():
        #             raise Exception('tried to add same line %i - %i n_par = %i twice!'
        #                             % (nfc, ntc, par_num))
        #         else:
        #             self.lines_index[nfc][ntc][par_num] = self._id
        #     else:
        #         self.lines_index[nfc][ntc] = {par_num: self._id}
        # else:
        #     self.lines_index[nfc] = {ntc: {par_num: self._id}}
        # self.group_line_div = {}
        # self.group_line_flipped = {}

    @subscriptable
    @staticmethod
    def by_key(item):
        """get single Line instance by key"""
        return Line['key', item]

    @subscriptable
    @staticmethod
    def by_nodes(item):
        """get Dpg instance by code"""
        return Line['nodes', item]

    # @classmethod
    # def clear(cls):
    #     if cls.key:
    #         for key in cls.lst.keys():
    #             cls.lst[key] = {}
    #     else:
    #         cls.lst = {}
    #     cls.lines_index = {}

    def __repr__(self):
        return '<Line %i -> %i npar = %i>' \
                % (self.node_from_code, self.node_to_code, self.parallel_num)

    # @staticmethod
    # def get_line(node_from_code, node_to_code, num_par=None):
    #     """get single instance or multiple instances from complex index"""
    #     if node_from_code in Line.lines_index.keys():
    #         if node_to_code in Line.lines_index[node_from_code].keys():
    #             # если указан номер параллели - возвращаем эту ветку
    #             # иначе возвращаем подсписок всех параллелей
    #             if num_par is not None:
    #                 if num_par in Line.lines_index[node_from_code][node_to_code].keys():
    #                     return Line[Line.lines_index[node_from_code][node_to_code][num_par]]
    #                 else:
    #                     return None
    #             else:
    #                 return [Line[Line.lines_index[node_from_code][node_to_code][n]]
    #                         for n in Line.lines_index[node_from_code][node_to_code].keys()]
    #
    #         else:
    #             return None
    #     else:
    #         return None

    def get_line_hour_state(self, hour):
        """get line hour data state"""
        return self.hour_data[hour].is_line_on()

    def add_line_hour_data(self, ls_row):
        """add LineHourData instance"""
        self._check_hour_data(ls_row)
        hour = ls_row.hour
        self._hour_data[hour] = LineHourData(ls_row, self)

    def _check_hour_data(self, ls_row):
        """check hour data consistency"""
        for attr in ['kt_re', 'kt_im', 'div', 'type', 'area_code']:
            self._check_datum(attr, getattr(ls_row, attr))

    def _check_datum(self, attr, datum):
        """check single property consistency"""
        if getattr(self, attr) != datum:
            raise Exception('%s not consistent for line %i - %i n_par = %i'
                            % (attr, self.node_from_code, self.node_to_code, self.parallel_num))

    def get_eq_db_lines_data(self):
        """get eq_db_lines view data"""
        if not self.eq_db_lines_data:
            self.prepare_lines_data()
        return self.eq_db_lines_data

    def prepare_lines_data(self):
        """prepare eq_db_lines view data"""
        for l_hd in self.hour_data:
            if not self.node_from or not self.node_to:
                print('ERROR! line %i-%i has no node(s)' % (self.node_from_code, self.node_to_code))
            if l_hd.state and self.node_from.get_node_hour_state(l_hd.hour) \
                        and self.node_to.get_node_hour_state(l_hd.hour):
                if not self.type:
                    node_start = self.node_from_code
                    node_finish = self.node_to_code
                    base_coeff = 0
                    k_pu = 0
                else:
                    node_start = self.node_to_code
                    node_finish = self.node_from_code
                    base_coeff = self.node_to.voltage_class / self.node_from.voltage_class
                    k_pu = math.sqrt(math.pow(self.kt_re, 2) + math.pow(self.kt_im, 2))
                lag = math.atan(self.kt_im / self.kt_re) if self.kt_re else 0

                self.eq_db_lines_data.append((
                    l_hd.hour, node_start, node_finish, self.parallel_num, self.type,
                    max(self.node_from.voltage_class, self.node_to.voltage_class), base_coeff,
                    l_hd.r, l_hd.x, l_hd.g, -l_hd.b, k_pu, lag, -l_hd.b_from, -l_hd.b_to
                ))
