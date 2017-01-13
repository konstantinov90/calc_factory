"""Class PriceZone."""
import itertools
from ..meta_base import MetaBase
from ..dpgs.dpg_demand import DpgDemand
from ..nodes.nodes import Node


class PriceZone(object, metaclass=MetaBase):
    """"class PriceZone"""
    seq = itertools.count()
    def __init__(self, _hour, _price_zone_code):
        self._id = next(self.seq)
        self.hour = _hour
        self.code = _price_zone_code
        self._cons = None
        self._init_on_load()

    def _init_on_load(self):
        """additional initialization"""
        if self._id not in self.lst.keys():
            self.lst[self._id] = self

    def get_consumption(self):
        """get sum consumption for PriceZone"""
        if not self._cons:
            self._cons = sum(dpg.get_con_value(self.hour, self.code) for dpg in DpgDemand
                             if not dpg.supply_gaes) + \
                         sum(node.get_fixed_con_value(self.hour) for node in Node
                             if node.get_node_hour_state(self.hour) \
                             and node.get_price_zone_code() == self.code)
        return (self.hour, self.code, self._cons)
