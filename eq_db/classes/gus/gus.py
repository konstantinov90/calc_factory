"""Class Gu."""
from operator import attrgetter
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
