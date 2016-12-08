from sqlalchemy import *

from utils.ORM import Base
from sql_scripts import rastr_load_script as rl


class LoadNodeHourData(Base):
    __tablename__ = 'loads_node_hour_data'
    id = Column(Integer, ForeignKey('loads_node_data.id'), primary_key=True)
    hour = Column(Integer, primary_key=True)
    pn = Column(Numeric)
    node_dose = Column(Numeric)
    node_state = Column(Integer)
    k_distr = Column(Numeric)
    pn_dpg_node_share = Column(Numeric)
    pdem_dpg_node_share = Column(Numeric)

    def __init__(self, rl_row, id):
        self.id = id
        self.hour = rl_row[rl['hour']]
        self.pn = rl_row[rl['pn']]
        self.node_dose = rl_row[rl['node_dose']]
        self.node_state = rl_row[rl['node_state']]
        self.k_distr = None
        self.pn_dpg_node_share = None
        self.pdem_dpg_node_share = None

    def set_k_distr(self, value):
        self.k_distr = value

    def set_pn_dpg_node_share(self, value):
        self.pn_dpg_node_share = value

    def set_pdem_dpg_node_share(self, value):
        self.pdem_dpg_node_share = value
