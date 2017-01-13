"""Class LoadHourData."""


class LoadHourData(object):
    """class LoadHourData"""
    def __init__(self, rl_row):
        self.hour, _, self.consumer_code, *_ = rl_row
        self.pn = 0
        self.sum_node_dose = 0
        self.nodes_on = 0

    def __repr__(self):
        return '<LoadHourData: %i>' % self.hour
