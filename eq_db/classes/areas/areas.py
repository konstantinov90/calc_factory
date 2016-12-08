from sqlalchemy import *
from sqlalchemy.orm import relationship, reconstructor

from utils.ORM import Base, MetaBase
from sql_scripts import rastr_areas_script as ra

# from eq_db.classes.nodes import NodesList
from .areas_hour_data import AreaHourData
# from ..nodes import Node

HOURCOUNT = 24

class Area(Base, metaclass=MetaBase):
    __tablename__ = 'areas'
    code = Column(Integer, primary_key=True)

    hour_data = relationship('AreaHourData', order_by='AreaHourData.hour', lazy='subquery')
    nodes = relationship('Node', primaryjoin='Node.area_code == Area.code')
    impex_data = relationship('ImpexArea', uselist=False, primaryjoin='Area.code == ImpexArea.area_code', foreign_keys='ImpexArea.area_code')
    # nodes = relationship('Node', back_populates='area')

    def __init__(self, ra_row):
        self.code = ra_row[ra['area']]
        self.area_hour_data = {}
        # self.nodes = NodesList()
        self._init_on_load()

    @reconstructor
    def _init_on_load(self):
        if self.code not in self.lst.keys():
            self.lst[self.code] = self
        # self.sum_pn_retail_diff = [0 for hour in range(HOURCOUNT)]
        # self.nodes_on = [0 for hour in range(HOURCOUNT)]

    def add_area_hour_data(self, ra_row):
        hour = ra_row[ra['hour']]
        if hour in self.area_hour_data.keys():
            raise Exception('tried to add area %i hour twice!' % self.code)
        self.area_hour_data[hour] = AreaHourData(ra_row)

    def get_sum_pn_retail_diff(self, hour):
        return sum(map(lambda node: max(node.hour_data[hour].pn - node.hour_data[hour].retail, 0) if node.hour_data[hour].state else 0, self.nodes))

    def get_nodes_on(self, hour):
        return sum(map(lambda node: 1 if node.hour_data[hour].state else 0, self.nodes))

    def attach_nodes(self, nodes_list):
        for node in nodes_list.get_nodes_by_area(self.code):
            self.nodes.append(node)
            for hour, hd in node.hour_data.items():
                self.sum_pn_retail_diff[hour] += max(hd.pn - hd.retail, 0) * hd.is_node_on()
                self.nodes_on[hour] += hd.is_node_on()
        for node in self.nodes:
            for hd in node.hour_data:
                k_distr = (max(hd.pn - hd.retail, 0) * (1 if hd.state else 0) / self.get_sum_pn_retail_diff(hd.hour)) if self.get_sum_pn_retail_diff(hd.hour) else ((hd.is_node_on() / self.get_nodes_on(hour)) if self.get_nodes_on(hour) else 0)
                node.set_k_distr(hour, k_distr)
