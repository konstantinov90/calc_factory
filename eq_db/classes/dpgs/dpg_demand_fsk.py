"""Class DpgDemandFSK."""
from .dpg_demand import DpgDemand


class DpgDemandFSK(DpgDemand):
    """class DpgDemandFSK"""

    lst = {'id': {}, 'code': {}}
    def _init_on_load(self):
        """additional initialization"""
        super()._init_on_load()
        if self._id not in self.lst['id']:
            self.lst['id'][self._id] = self
        if self.code not in self.lst['code']:
            self.lst['code'][self.code] = self

    def distribute_bid(self):
        """overriding abstract method"""

    def prepare_demand_response_data(self):
        """prepare demand response data"""
