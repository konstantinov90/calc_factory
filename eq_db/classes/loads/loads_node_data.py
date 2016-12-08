import itertools
from sqlalchemy import *
from sqlalchemy.orm import relationship, reconstructor

from utils.ORM import Base
from sql_scripts import rastr_load_script as rl

from .loads_node_hour_data import LoadNodeHourData

class LoadNodeData(Base):
    __tablename__ = 'loads_node_data'
    id = Column(Integer, unique=True)
    consumer_code = Column(Integer, ForeignKey('loads.consumer_code'), primary_key=True)
    node_code = Column(Integer, ForeignKey('nodes.code'), primary_key=True)

    hour_data = relationship('LoadNodeHourData', order_by='LoadNodeHourData.hour', lazy='subquery')
    node = relationship('Node', primaryjoin='Node.code == LoadNodeData.node_code')
    load = relationship('Load', back_populates='nodes_data')

    seq = itertools.count()

    def __init__(self, rl_row):
        self.id = next(self.seq)
        self.consumer_code = rl_row[rl['consumer_code']]
        self.node_code = rl_row[rl['node_code']]
        self.hours = {}

    # @reconstructor
    # def _init_on_load(self):

    def recalculate(self):
        for hd in self.hour_data:
            sum_node_dose = self.load.hour_data[hd.hour].sum_node_dose
            node_state = self.node.get_node_hour_state(hd.hour)
            if sum_node_dose:
                k_distr = self.hour_data[hd.hour].node_dose / sum_node_dose if node_state else 0
            else:
                nodes_on = self.load.hour_data[hd.hour].nodes_on
                if nodes_on:
                    k_distr = (1 if node_state else 0) / nodes_on
                else:
                    k_distr = 0
            hd.k_distr = k_distr

    def add_hour_data(self, rl_row):
        hour = rl_row[rl['hour']]
        if hour in self.hours.keys():
            raise Exception('tried to add load data for consmer %i at node %i twice!' % (self.consumer_code, self.node_code))
        self.hours[hour] = LoadNodeHourData(rl_row, self.id)
