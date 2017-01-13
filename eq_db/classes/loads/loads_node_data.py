"""Class LoadNodeData."""
import itertools
from operator import attrgetter
from .loads_node_hour_data import LoadNodeHourData


class LoadNodeData(object):
    """class LoadNodeData"""
    seq = itertools.count()

    def __init__(self, rl_row, load):
        self._id = next(self.seq)
        _, self.node_code, self.consumer_code, *_ = rl_row
        self.load = load
        self._hour_data = {}
        self.node = None

    @property
    def hour_data(self):
        """hour_data property"""
        return sorted(self._hour_data.values(), key=attrgetter('hour'))

    def __repr__(self):
        return '<LoadNodeData %i>' % self.node_code

    def set_node(self, nodes_list):
        """set Node instance"""
        self.node = nodes_list[self.node_code]
        self.node.add_load_node(self)

    def recalculate(self):
        """additional calculation (after model initialization)"""
        for _hd in self.hour_data:
            sum_node_dose = self.load.hour_data[_hd.hour].sum_node_dose
            node_state = self.node.get_node_hour_state(_hd.hour)
            if sum_node_dose:
                k_distr = self.hour_data[_hd.hour].node_dose / sum_node_dose if node_state else 0
            else:
                nodes_on = self.load.hour_data[_hd.hour].nodes_on
                if nodes_on:
                    k_distr = (1 if node_state else 0) / nodes_on
                else:
                    k_distr = 0
            _hd.k_distr = k_distr

    def add_hour_data(self, rl_row):
        """add LoadNodeHourData instance"""
        hour = rl_row.hour
        if hour in self._hour_data.keys():
            raise Exception('tried to add load data for consmer %i at node %i twice!'
                            % (self.consumer_code, self.node_code))
        self._hour_data[hour] = LoadNodeHourData(rl_row, self._id)
