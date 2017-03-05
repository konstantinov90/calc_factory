"""Class NodeHourData."""


class NodeHourData(object):
    """class NodeHourData"""
    def __init__(self, ns_row, parent):
        self.node = parent
        self.node_code, *_, self.hour, _, self.type, self.pn, \
            self.max_q, self.min_q, self.voltage, _, self.fixed_voltage, \
            self.g_shunt, self.b_shunt, self.qn, self.qg, self.pg, \
            self.q_shunt, self.p_shunt, self.b_shr = ns_row
        self.state = not ns_row.state
        self.phase = 3.1415 * (((ns_row.phase + 180) % 360) - 180) / 180
        self.pdem = 0
        self.retail = 0
        self.k_distr = None
        self.balance_node_code = None

    def __repr__(self):
        return '<NodeHourData: %i -> %s>' % (self.hour, ('ON' if self.state else 'OFF'))

    def set_balance_node(self, node_code):
        """set balance node code"""
        self.balance_node_code = node_code

    def is_node_on(self):
        """get node state at instance's hour"""
        return self.state

    def turn_on(self):
        """turn node on at instance's hour"""
        self.state = True

    def turn_off(self):
        """turn node hour off"""
        self.state = False
        # print('%r turned off at hour %i' % (self.node, self.hour))
        for line in self.node.lines:
            line.hour_data[self.hour].turn_off()

    def is_balance_node(self):
        """determine, node is balance node at instance's hour"""
        return not self.type

    def get_eq_db_node_hour(self):
        """get eq_db_nodes view data"""
        return self.voltage, self.phase

    def add_to_pdem(self, value):
        """add volume to pdem"""
        self.pdem += value

    def add_to_retail(self, value):
        """add volume to retail"""
        self.retail += value

    def set_k_distr(self, value):
        """set k_distr"""
        self.k_distr = value
