"""Class Load."""
from operator import attrgetter
from ..meta_base import MetaBase
from .loads_node_data import LoadNodeData
from .loads_hour_data import LoadHourData

HOURCOUNT = 24


class Load(object, metaclass=MetaBase):
    """class Load"""
    def __init__(self, rl_row, is_new=False):
        self.consumer_code = rl_row.consumer_code
        self.is_new = is_new
        self._nodes_data = {}
        self._hour_data = {}
        self.dpg = None
        self._init_on_load()

    @property
    def hour_data(self):
        """hour_data property"""
        return sorted(self._hour_data.values(), key=attrgetter('hour'))

    @property
    def nodes_data(self):
        """nodes_data property"""
        return list(self._nodes_data.values())

    def _init_on_load(self):
        """additional initialization"""
        if self.consumer_code not in self.lst.keys():
            self.lst[self.consumer_code] = self

    def __repr__(self):
        return '<Load %i>' % self.consumer_code

    def set_dpg(self, dpg):
        """set DpgDemandLoad instance"""
        self.dpg = dpg

    def set_nodes(self, nodes_list):
        """set Node instance for each LoadNodeData instance"""
        for _nd in self.nodes_data:
            _nd.set_node(nodes_list)

    def get_sum_node_dose(self, hour):
        """get sum LoadNodeHourData instance's dose"""
        return sum(_nd.hour_data[hour].node_dose if _nd.node.get_node_hour_state(hour) else 0
                   for _nd in self.nodes_data)

    def get_sum_node_pn(self, hour):
        """get sum LoadNodeHourData instance's pn"""
        return sum(_nd.hour_data[hour].pn if _nd.node.get_node_hour_state(hour) else 0
                   for _nd in self.nodes_data)

    def get_nodes_on(self, hour):
        """get LoadNodeHourData turned on nodes count"""
        return sum(1 if _nd.node.get_node_hour_state(hour) else 0 for _nd in self.nodes_data)

    def recalculate(self):
        """additional recalculation after model initialization"""
        for _hd in self.hour_data:
            _hd.sum_node_dose = self.get_sum_node_dose(_hd.hour)
            _hd.nodes_on = self.get_nodes_on(_hd.hour)
            _hd.pn = self.get_sum_node_pn(_hd.hour)
        for _nd in self.nodes_data:
            _nd.recalculate()

    def add_load_hour_data(self, rl_row):
        """set LoadHourData and LoadNodeData instances"""
        hour = rl_row.hour
        node_code = rl_row.node_code
        if not node_code in self._nodes_data.keys():
            self._nodes_data[node_code] = LoadNodeData(rl_row, self)
        self._nodes_data[node_code].add_hour_data(rl_row)
        if not hour in self._hour_data.keys():
            self._hour_data[hour] = LoadHourData(rl_row)
