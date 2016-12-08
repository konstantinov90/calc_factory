from sqlalchemy import *
from sqlalchemy.orm import reconstructor
from sqlalchemy.ext.declarative import declared_attr

class Dpg(object):
    id = Column(Integer, primary_key=True)
    code = Column(String(16), unique=True)
    is_unpriced_zone = Column(Integer)

    __table_args__ = (
        CheckConstraint('is_unpriced_zone between 0 and 3', name='unpriced_zone_check'),
    )

    #static data
    max_bid_prices = {}

    def __init__(self, id, code, is_unpriced_zone):
        self.id = id
        self.code = code
        self.is_unpriced_zone = is_unpriced_zone
        self._init_on_load()

    lst = {'id': {}, 'code': {}}
    @reconstructor
    def _init_on_load(self):
        if self.id not in Dpg.lst['id']:
            Dpg.lst['id'][self.id] = self
        if self.code not in Dpg.lst['code']:
            Dpg.lst['code'][self.code] = self
        self.distributed_bid = []

    def serialize(self, session):
        session.add(self)

    def recalculate(self):
        pass

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.code)

    # @staticmethod
    # def set_max_bid_prices(max_prices):
    #     for hour, price in max_prices:
    #         if hour in Dpg.max_bid_prices.keys():
    #             raise Exception('too many max prices!')
    #         Dpg.max_bid_prices[hour] = price

    # @staticmethod
    # def get_max_bid_price(hour):
    #     return self.max_bid_prices[hour]

    def get_distributed_bid(self):
        if not self.distributed_bid:
            self.distribute_bid()
        return self.distributed_bid

    def get_con_value(self, hour, price_zone_code):
        return 0

    def distribute_bid(self):
        pass

    def set_station(self, station):
        pass

    def get_fixedgen_data(self):
        return []

    def get_fixedcon_data(self):
        return []

    def prepare_fixedgen_data(self, nodes_list):
        pass

    def attach_to_fed_station(self, dpg_list):
        pass

    def prepare_fixedcon_data(self):
        pass

    def attach_sections(self, sections_list):
        pass

    def set_area(self, areas_list):
        pass

    def set_consumer(self, consumers_list):
        pass

    def set_load(self, loads_list):
        pass
