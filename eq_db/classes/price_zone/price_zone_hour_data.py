"""Class PriceZoneHourData"""
from ..dpgs.dpg_demand import DpgDemand
from ..nodes.nodes import Node


class PriceZoneHourData(object):
    def __init__(self, hour, price_zone_code):
        self.hour = hour
        self.price_zone_code = price_zone_code
        self._cons = None

    def __repr__(self):
        return '<PriceZoneHourData %i: %i>' % (self.price_zone_code, self.hour)

    def get_consumption(self):
        """get sum consumption for PriceZoneHourData"""
        if not self._cons:
            self._cons = sum(dpg.get_con_value(self.hour, self.price_zone_code)
                             for dpg in DpgDemand if not dpg.supply_gaes) + \
                         sum(node.get_fixed_con_value(self.hour) for node in Node
                             if node.get_node_hour_state(self.hour) \
                             and node.get_price_zone_code() == self.price_zone_code)
        return (self.hour, self.price_zone_code, self._cons)
