"""Class LoadNodeHourData."""


class LoadNodeHourData(object):
    """class LoadNodeHourData"""
    def __init__(self, rl_row, _id):
        self.load_node_id = _id
        self.hour, *_, self.pn, self.node_dose, self.node_state = rl_row
        self.k_distr = None
        self.pn_dpg_node_share = None
        self.pdem_dpg_node_share = None

    def __repr__(self):
        return '<LoadNodeHourData: %i>' % self.hour

    def set_k_distr(self, value):
        """set k_distr for DpgDemandLoad"""
        self.k_distr = value

    def set_pn_dpg_node_share(self, value):
        """set Dpg pn share for node"""
        self.pn_dpg_node_share = value

    def set_pdem_dpg_node_share(self, value):
        """set Dpg pdem share for node"""
        self.pdem_dpg_node_share = value
