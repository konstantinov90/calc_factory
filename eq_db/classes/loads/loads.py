from sqlalchemy import *
from sqlalchemy.orm import relationship, reconstructor

from utils.ORM import Base, MetaBase
from sql_scripts import rastr_load_script as rl
from .loads_node_data import LoadNodeData
from .loads_hour_data import LoadHourData

HOURCOUNT = 24


class Load(Base, metaclass=MetaBase):
    __tablename__ = 'loads'
    consumer_code = Column(Integer, primary_key=True)

    nodes_data = relationship('LoadNodeData', back_populates='load')
    hour_data = relationship('LoadHourData', order_by='LoadHourData.hour', lazy='subquery')
    dpg = relationship('DpgDemandLoad', uselist=False, primaryjoin='DpgDemandLoad.consumer_code == Load.consumer_code', foreign_keys='DpgDemandLoad.consumer_code')

    def __init__(self, rl_row):
        self.consumer_code = rl_row[rl['consumer_code']]
        self.nodes = {}
        self.hours = {}
        # self.sum_pn = [0 for hour in range(HOURCOUNT)]
        # self.sum_node_dose = [0 for hour in range(HOURCOUNT)]
        # self.nodes_on = [0 for hour in range(HOURCOUNT)]

    @reconstructor
    def _init_on_load(self):
        if self.consumer_code not in self.lst.keys():
            self.lst[self.consumer_code] = self

    def get_sum_node_dose(self, hour):
        return sum(map(lambda node_data: node_data.hour_data[hour].node_dose if node_data.node.get_node_hour_state(hour) else 0, self.nodes_data))

    def get_nodes_on(self, hour):
        return sum(map(lambda node_data: 1 if node_data.node.get_node_hour_state(hour) else 0, self.nodes_data))

    def serialize(self, session):
        session.add(self)
        session.add_all(self.hours.values())
        session.add_all(self.nodes.values())
        for node in self.nodes.values():
            session.add_all(node.hours.values())
        session.flush()

    def recalculate(self):
        for hd in self.hour_data:
            hd.sum_node_dose = self.get_sum_node_dose(hd.hour)
            hd.nodes_on = self.get_nodes_on(hd.hour)
        for nd in self.nodes_data:
            nd.recalculate()

    def add_load_hour_data(self, rl_row):
        hour = rl_row[rl['hour']]
        node_code = rl_row[rl['node_code']]
        if not node_code in self.nodes.keys():
            self.nodes[node_code] = LoadNodeData(rl_row)
        self.nodes[node_code].add_hour_data(rl_row)

        if not hour in self.hours.keys():
            self.hours[hour] = LoadHourData(rl_row)
        self.hours[hour].pn += self.nodes[node_code].hours[hour].pn
        # self.sum_node_dose[hour] += load_hour_data.node_dose

    def attach_nodes(self, nodes_list):
        for node_code in self.nodes.keys():
            node = nodes_list[node_code]
            self.nodes[node_code]['node_obj'] = node
            for hour, hd in self.nodes[node_code]['hour_data'].items():
                node_state = node.get_node_hour_state(hour)
                self.sum_node_dose[hour] += hd.node_dose * node_state
                self.nodes_on[hour] += node_state
        for node_data in self.nodes.values():
            node = node_data['node_obj']
            for hour, hd in node_data['hour_data'].items():
                node_state = node.get_node_hour_state(hour)
                k_distr = (hd.node_dose * node_state / self.sum_node_dose[hour]) if self.sum_node_dose[hour] else ((node_state / self.nodes_on[hour]) if self.nodes_on[hour] else 0)
                hd.set_k_distr(k_distr)
