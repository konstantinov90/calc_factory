"""PriceZone hourly data."""
from sqlalchemy import Column, Integer, UniqueConstraint
from sqlalchemy.orm import reconstructor
from utils.ORM import Base, MetaBase
from ..dpgs.dpg_demand import DpgDemand
from ..nodes.nodes import Node


class PriceZone(Base, metaclass=MetaBase):
    __tablename__ = 'price_zone_data'
    id = Column(Integer, primary_key=True)
    hour = Column(Integer)
    code = Column(Integer)

    UniqueConstraint('hour', 'code', name='upz_1')

    def __init__(self, _hour, _price_zone_code):
        self.hour = _hour
        self.code = _price_zone_code

    @reconstructor
    def _init_on_load(self):
        if self.id not in self.lst.keys():
            self.lst[self.id] = self

    def serialize(self, session):
        session.add(self)
        session.flush()

    def get_consumption(self):
        cons = 0
        for dpg in DpgDemand:
            cons += dpg.get_con_value(self.hour, self.code)
        for node in Node:
            if node.get_node_hour_state(self.hour) \
                and node.get_price_zone_code() == self.code:
                cons += node.get_fixed_con_value(self.hour)
        return (self.hour, self.code, cons)
