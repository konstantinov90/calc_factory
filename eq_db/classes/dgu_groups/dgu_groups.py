"""Class DguGroup."""
from operator import attrgetter
from itertools import product
from ..meta_base import MetaBase
from .dgu_groups_dgu_data import DguGroupDguData
from .dgu_groups_hour_data import DguGroupHourData


class DguGroup(object, metaclass=MetaBase):
    """class DguGroup"""
    def __init__(self, rg_row):
        self.code = rg_row.group_code
        self.dgus = {}
        self.hours = {}
        self._init_on_load()

    @property
    def hour_data(self):
        """hour_data property"""
        return sorted(self.hours.values(), key=attrgetter('hour'))

    @property
    def dgu_data(self):
        """dgu_data property"""
        return list(self.dgus.values())

    def _init_on_load(self):
        """additional initialization"""
        if self.code not in self.lst.keys():
            self.lst[self.code] = self
        self.constraint_data = []
        self.prepared_dgu_data = []

    def add_dgu(self, rg_row):
        """add DguGroupDguData instance"""
        dgu_code = rg_row.rge_code
        if dgu_code in self.dgus.keys():
            raise Exception('tried to add same dgu to group %i twice!' % self.code)
        self.dgus[dgu_code] = DguGroupDguData(rg_row)

    def add_reserve_data(self, prs_row):
        """add DguGroupHourData instance"""
        hour = prs_row.hour
        if hour in self.hours.keys():
            raise Exception('tried to add same reserve to group %i twice!' % self.code)
        self.hours[hour] = DguGroupHourData(prs_row)

    def get_constraint_data(self):
        """get eq_db_group_constraints view data"""
        if not self.constraint_data:
            self.prepare_constraint_data()
        return self.constraint_data

    def prepare_constraint_data(self):
        """prepare eq_db_group_constraints view data"""
        for _hour_data in self.hour_data:
            if not _hour_data.state:
                continue
            self.constraint_data.append((
                _hour_data.hour, self.code, _hour_data.p_max, _hour_data.p_min
            ))

    def get_dgu_data(self):
        """get eq_db_group_constraint_rges view data"""
        if not self.prepared_dgu_data:
            self.prepare_dgu_data()
        return self.prepared_dgu_data

    def prepare_dgu_data(self):
        """prepare eq_db_group_constraint_rges view data"""
        turned_on_hour_data = [_hour_data for _hour_data in self.hour_data if _hour_data.state]
        if not turned_on_hour_data:
            return
        for _hour_data, dgu in product(turned_on_hour_data, self.dgu_data):
            self.prepared_dgu_data.append((
                _hour_data.hour, self.code, float(dgu.dgu_code)
            ))
