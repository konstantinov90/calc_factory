"""Class Gu."""
from operator import attrgetter
import constants as C
from utils.subscriptable import subscriptable
from ..meta_base import MetaBase


class Gu(object, metaclass=MetaBase):
    """class Gu"""
    def __init__(self, gs_row, is_new=False):
        self.is_in_rio = True
        # self.is_remove = False
        self._id, self.code, self.dgu_id, self.fuel_types, self.fixed_power = gs_row
        self.bid_factor = 1
        if self.fuel_types:
            self.fuel_type_list = [int(fuel_type) for fuel_type in self.fuel_types.split(',')]
        else:
            self.fuel_type_list = []

        self.is_new = is_new
        self._hour_data = {}
        self.dgu_code = None
        self.dgu = None
        self.avg_pmax = None
        self.avg_pmin = None
        self._init_on_load()

    @classmethod
    def from_nblock(cls, ns_row):
        """from rastr_nblock row constructor"""
        instance = cls((ns_row.gu_code,) * 2 + (None,) * 3)
        instance.is_in_rio = False
        return instance

    @property
    def hour_data(self):
        """hour_data property"""
        return sorted(self._hour_data.values(), key=attrgetter('hour'))

    lst = {'id': {}, 'code': {}}
    key = 'id'
    def _init_on_load(self):
        """additional initialization"""
        if self._id not in self.lst['id'].keys():
            self.lst['id'][self._id] = self
        if self.code not in self.lst['code'].keys():
            self.lst['code'][self.code] = []
        if self not in self.lst['code'][self.code]:
            self.lst['code'][self.code].append(self)

    @subscriptable
    @staticmethod
    def by_code(item):
        """get Gu instances by code"""
        return Gu['code', item]

    @subscriptable
    @staticmethod
    def by_id(item):
        """get Gu instance by id"""
        return Gu['id', item]

    def __repr__(self):
        return '<GU id: %r / code: %r (Dgu: %r), fuel: %r, power:%r>' \
            % (self._id, self.code, self.dgu_code, self.fuel_types, self.fixed_power)

    # def set_to_remove(self):
    #     """set Gu to remove"""
    #     self.is_remove = True

    def set_parent_dgu(self, dgus_list):
        """set Dgu instance"""
        if self.is_in_rio:
            dgu = dgus_list.by_id[self.dgu_id]
            self.dgu_code = dgu.code
        else:
            dgu = dgus_list.by_code[self.dgu_code]
            self.dgu_id = dgu._id
        if dgu.code != self.dgu_code:
            raise Exception('GU %i trader RGE code not consistent with rastr rge_Code!'
                            % self.code)
            # print('GU %i trader RGE code not consistent with rastr rge_Code!' % self.code)
        self.dgu = dgu
        try:
            dgu.add_gu(self)
        except Exception:
            print(self.dgu_code, self.code, self.dgu_id)
            raise Exception('Gu!')

    def add_gu_hour_data(self, ns_row, gu_hour_data):
        """add GuHourData instance"""
        dgu_code = ns_row.dgu_code
        hour = ns_row.hour
        if not self.dgu_code:
            self.dgu_code = dgu_code
        if self.dgu_code != dgu_code:
            raise Exception('dgu_code for GU %i not consistent!' % self.code)
        if hour in self._hour_data.keys():
            raise Exception('gu with code %i has double %i hour' % (self.code, hour))
        self._hour_data[hour] = gu_hour_data

    def recalculate(self):
        """additional calculation after initialization"""
        sum_state = sum(hd.state_init for hd in self.hour_data)

        if not self.code in [_['dep'] for _ in C.dependant_gus]:
            sum_pmin = sum(hd.state_init * hd.pmin for hd in self.hour_data)
            sum_pmax = sum(hd.state_init * hd.pmax for hd in self.hour_data)
            # self.avg_pmin = sum_pmin / sum_state if sum_state else 0
            # self.avg_pmax = sum_pmax / sum_state if sum_state else 0

            if not any(hd.changed for hd in self.hour_data):
                return
            self.avg_pmin = max(hd.state_init * hd.pmin for hd in self.hour_data)
            self.avg_pmax = max(hd.state_init * hd.pmax for hd in self.hour_data)
            for _hd in self.hour_data:
                pmax_hour = _hd.pmax * _hd.state_init
                avg_pmax_hour = self.avg_pmax * _hd.state_init
                if abs(pmax_hour - avg_pmax_hour) > 0 * avg_pmax_hour:
                    _hd.delta_pmin = (self.avg_pmin  - _hd.pmin) * _hd.state_init
                    _hd.delta_pmax = avg_pmax_hour - pmax_hour
                    print('Gu %i has delta (%.3f, %.3f) at hour %i'
                          % (self.code, _hd.delta_pmin, _hd.delta_pmax, _hd.hour))
        else:
            (base_gus,) = [_['base'] for _ in C.dependant_gus if _['dep'] == self.code]
            base_gus_hour_data = []
            for code in base_gus:
                (g_unit,) = Gu.by_code[code]
                base_gus_hour_data.append(g_unit.hour_data)
            hds = list(zip(*base_gus_hour_data))

            sum_pmin = sum_pmax = 0
            self.avg_pmin = self.avg_pmax = 0
            for _hd in self.hour_data:
                ratio_init = sum(hd.state_init for hd in hds[_hd.hour]) / len(hds[_hd.hour])
                sum_pmin += _hd.pmin / ratio_init if ratio_init else 0
                sum_pmax += _hd.pmax / ratio_init if ratio_init else 0
                self.avg_pmin = max(self.avg_pmin, _hd.pmin / ratio_init if ratio_init else 0)
                self.avg_pmax = max(self.avg_pmax, _hd.pmax / ratio_init if ratio_init else 0)
            # self.avg_pmin = sum_pmin / sum_state if sum_state else 0
            # self.avg_pmax = sum_pmax / sum_state if sum_state else 0

            for _hd in self.hour_data:
                hour = _hd.hour
                ratio = sum(hd.state for hd in hds[hour]) / len(hds[hour])
                ratio_init = sum(hd.state_init for hd in hds[hour]) / len(hds[hour])
                if not ratio:
                    _hd.pmax = _hd.pmin = 0
                    _hd.state = False
                elif (_hd.state and _hd.changed) or any(hd.changed for _ in hds for hd in _):
                    _hd.pmax = (self.avg_pmax or _hd.pmax_t) * ratio
                    _hd.pmin = (self.avg_pmin or _hd.pmin_t) * ratio
                    _hd.changed = True
                    _hd.state = bool(ratio) and _hd.state
