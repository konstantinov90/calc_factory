from time import time
from operator import itemgetter
from sqlalchemy import *
from sqlalchemy.orm import relationship, reconstructor

from sql_scripts import bid_pair_script as bps
from sql_scripts import consumers_script as cs
from sql_scripts import rastr_consumer_script as rs

from utils.ORM import Base, MetaBase
from .base_dpg import Dpg
from .dpg_disqualified import DpgDisqualified

HOURCOUNT = 24
UNPRICED_AREA = ('PCHITAZN', 'PAMUREZN')


class DpgDemand(Dpg, Base, metaclass=MetaBase):
    __tablename__ = 'dpg_demands'
    # code inherited
    # selfid dds_rowed
    # is_unpriced_zone inherited
    type = Column(String(6))
    consumer_code = Column(Integer, unique=True)
    is_system = Column(Boolean)
    is_fsk = Column(Boolean)
    is_fed_station = Column(Boolean)
    is_disqualified = Column(Boolean)
    area_code = Column(BigInteger)
    min_forecast = Column(Numeric)
    max_forecast = Column(Numeric)

    consumer = relationship('Consumer', uselist=False, primaryjoin='DpgDemand.consumer_code == Consumer.code', foreign_keys='Consumer.code')
    load = relationship('Load', uselist=False, primaryjoin='DpgDemand.consumer_code == Load.consumer_code', foreign_keys='Load.consumer_code')
    area = relationship('Area', uselist=False, primaryjoin='Area.code == DpgDemand.area_code', foreign_keys='Area.code')
    supply_dpgs = relationship('DpgSupply', back_populates='fed_station')
    bid = relationship('Bid', foreign_keys='Bid.dpg_id', uselist=False, primaryjoin='Bid.dpg_id == DpgDemand.id')
    disqualified_data = relationship('DpgDisqualified', uselist=False)

    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': None
    }

    def __init__(self, cs_row):
        super().__init__(cs_row[cs['dpg_id']], cs_row[cs['dpg_code']], cs_row[cs['is_unpriced_zone']])
        self.type = self.polymorphic_identity
        self.consumer_code = cs_row[cs['consumer_code']]
        self.is_system = True if cs_row[cs['is_system']] else False
        self.is_fsk = True if cs_row[cs['is_fsk']] else False
        self.is_fed_station = True if cs_row[cs['is_fed_station']] else False
        self.is_disqualified = True if cs_row[cs['is_disqualified']] else False
        self.area_code = cs_row[cs['area']]
        # self.supply_dpgs = []
        self.prepared_fixedcon_data = []
        self.consumer_obj = None
        self.load_obj = None
        self.area_obj = None
        self.min_forecast = None if cs_row[cs['min_forecast']] is None else (cs_row[cs['min_forecast']] / 1000)
        self.max_forecast = 1000000 if cs_row[cs['max_forecast']] is None else (cs_row[cs['max_forecast']] / 1000)
        self.p_dem = [0 for hour in range(HOURCOUNT)]
        self.disqualified = None

    lst = {'id': {}, 'code': {}}
    @reconstructor
    def _init_on_load(self):
        super()._init_on_load()
        if self.id not in DpgDemand.lst['id']:
            DpgDemand.lst['id'][self.id] = self
        if self.code not in DpgDemand.lst['code']:
            DpgDemand.lst['code'][self.code] = self

    def serialize(self, session):
        super().serialize(session)
        if self.disqualified:
            session.add(self.disqualified)

    def get_con_value(self, hour, price_zone_code):
        if price_zone_code == 1:
            return sum(b[4] for b in self.get_distributed_bid() \
                        if b[0] == hour and b[3] <= 999999)
        elif price_zone_code == 2:
            return sum(b[4] for b in self.get_distributed_bid() \
                        if b[0] == hour and b[3] > 999999)
        else:
            raise Exception('Wrong price zone code!')

    def set_consumer(self, consumers_list):
        self.consumer_obj = consumers_list[self.consumer_code]
        if not self.consumer_obj:
            return
        for hour, hd in self.consumer_obj.consumer_hour_data.items():
            max_bid = self.max_forecast
            if self.bid:
                if self.bid[hour]:
                    bid_data = self.bid[hour][-1]  # last bid
                    max_bid = bid_data[bps['volume_init']] if bid_data[bps['volume_init']] else 1e15
            self.p_dem[hour] = min(max_bid, self.max_forecast, hd.pdem)

    def get_pdem(self, hour):
        max_bid = 1e15
        if self.bid:
            if self.bid.hour_data[hour]:
                if self.bid.hour_data[hour].interval_data:
                    bid = self.bid.hour_data[hour].interval_data[-1]
                    max_bid = bid.volume_init if bid.volume_init else 1e15
        return min(max_bid, self.max_forecast, self.consumer.hour_data[hour].pdem)

    def set_load(self, loads_list):
        self.load_obj = loads_list[self.consumer_code]

    def set_area(self, areas_list):
        self.area_obj = areas_list[self.area_code]

    def add_supply_dpg(self, supply_dpg):
        self.supply_dpgs.append(supply_dpg)

    def get_fixedcon_data(self):
        if not self.prepared_fixedcon_data:
            self.prepare_fixedcon_data()
        return self.prepared_fixedcon_data

    # @staticmethod
    def add_disqualified_data(self, dds_row):
        self.disqualified = DpgDisqualified(dds_row)

    # @staticmethod
    # def get_disqualified_data():
    #     return DpgDemand.disqualified_data

    def prepare_fixedcon_data(self):
        pass
