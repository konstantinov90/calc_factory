from sqlalchemy import *
from sqlalchemy.orm import relationship

from utils.ORM import Base
from sql_scripts import nodes_script as ns


class NodeHourData(Base):
    __tablename__ = 'nodes_hour_data'
    node_code = Column(Integer, ForeignKey('nodes.code'), primary_key=True)
    hour = Column(Integer, primary_key=True)
    state = Column(Boolean)
    type = Column(Integer)
    voltage = Column(Numeric)
    fixed_voltage = Column(Numeric)
    phase = Column(Numeric)
    pn = Column(Numeric)
    pg = Column(Numeric)
    qn = Column(Numeric)
    qg = Column(Numeric)
    min_q = Column(Numeric)
    max_q = Column(Numeric)
    g_shunt = Column(Numeric)
    b_shunt = Column(Numeric)
    p_shunt = Column(Numeric)
    q_shunt = Column(Numeric)
    b_shr = Column(Numeric)
    pdem = Column(Numeric)
    retail = Column(Numeric)
    k_distr = Column(Numeric)
    balance_node_code = Column(Integer)

    # node = relationship('Node', back_populates='hours')

    def __init__(self, ns_row):
        self.node_code = ns_row[ns['node_code']]
        self.hour = ns_row[ns['hour']]
        self.state = True if not ns_row[ns['state']] else False
        self.type = ns_row[ns['type']]
        self.voltage = ns_row[ns['voltage']]
        self.fixed_voltage = ns_row[ns['fixed_voltage']]
        self.phase = 3.1415 * (((ns_row[ns['phase']] + 180) % 360) - 180) / 180
        self.pn = ns_row[ns['pn']]
        self.pg = ns_row[ns['pg']]
        self.qn = ns_row[ns['qn']]
        self.qg = ns_row[ns['qg']]
        self.min_q = ns_row[ns['min_q']]
        self.max_q = ns_row[ns['max_q']]
        self.g_shunt = ns_row[ns['g_shunt']]
        self.b_shunt = ns_row[ns['b_shunt']]
        self.p_shunt = ns_row[ns['p_shunt']]
        self.q_shunt = ns_row[ns['q_shunt']]
        self.b_shr = ns_row[ns['b_shr']]
        self.pdem = 0
        self.retail = 0
        self.k_distr = None
        self.balance_node_code = None

    def set_balance_node(self, node_code):
        self.balance_node_code = node_code

    def is_node_on(self):
        return self.state

    def turn_on(self):
        self.state = True

    def is_balance_node(self):
        return not self.type

    def get_eq_db_node_hour(self):
        return self.voltage, self.phase

    def add_to_pdem(self, value):
        self.pdem += value

    def add_to_retail(self, value):
        self.retail += value

    def set_k_distr(self, value):
        self.k_distr = value
