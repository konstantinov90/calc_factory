import itertools, traceback, re
from operator import itemgetter
from sqlalchemy import *
from sqlalchemy.orm import relationship, reconstructor

from utils.ORM import Base, MetaBase
from .base_dpg import Dpg

from sql_scripts import wsumgen_script as ws
from sql_scripts import bid_pair_script as bps
from sql_scripts import generators_script as gs


HOURCOUNT = 24
HYDROSTATIONTYPE = 2
HYDROINTERVAL = 0
PMINTECHPRICE = 0
PMINTECHINTERVAL = -20
PMINPRICE = 0.01
PMININTERVAL = -18
TARIFF = 9999
FORCEDSMOOTH = 0
PRICEACC = 0.8
PINTSCHGASTFUEL = 7


class DpgSupply(Dpg, Base, metaclass=MetaBase):
    __tablename__ = 'dpg_supplies'
    # code inherited
    # id inherited
    # is_unpriced_zone inherited
    is_gaes = Column(Boolean)
    is_blocked = Column(Boolean)
    is_pintsch_gas = Column(Boolean, nullable=True)
    fed_station_id = Column(Integer, ForeignKey('dpg_demands.id'))
    station_id = Column(Integer, ForeignKey('stations.id'))

    fed_station = relationship('DpgDemand', back_populates='supply_dpgs')
    station = relationship('Station', back_populates='dpgs')
    dgus = relationship('Dgu', back_populates='dpg')
    bid = relationship('Bid', foreign_keys='Bid.dpg_id', uselist=False, primaryjoin='Bid.dpg_id == DpgSupply.id')

    def __init__(self, gs_row):
        super().__init__(gs_row[gs['gtp_id']], gs_row[gs['dpg_code']], gs_row[gs['is_unpriced_zone']])
        self.is_gaes = True if gs_row[gs['is_gaes']] else False
        self.is_blocked = True if gs_row[gs['is_blocked']] else False
        self.is_pintsch_gas = None
        self.fed_station_id = gs_row[gs['fed_station_id']]
        self.station_id = gs_row[gs['station_id']]
        self.dgu_list = []
        self.prepared_fixedgen_data = []
        self.sum_pmin = [0 for hour in range(HOURCOUNT)]
        self.sum_pmin_technological = [0 for hour in range(HOURCOUNT)]
        self.sum_preg = [0 for hour in range(HOURCOUNT)]
        self.sum_pmax = [0 for hour in range(HOURCOUNT)]

    lst = {'id': {}, 'code': {}}
    @reconstructor
    def _init_on_load(self):
        super()._init_on_load()
        if self.id not in self.lst['id']:
            self.lst['id'][self.id] = self
        if self.code not in self.lst['code']:
            self.lst['code'][self.code] = self

    def add_dgu(self, dgu):
        self.dgu_list.append(dgu)
        if not dgu.node:
            return
        for hour, hd in dgu.dgu_hour_data.items():
            if not (self.is_gaes and hd.p < 0):  # если это не ГАЭС в режиме потребления
                if not hd.pmin <= hd.p <= hd.pmax:  # и ПДГ вне пределов регулирования
                    continue
            if not dgu.node.get_node_hour_state(hour):  # если узел отключен
                continue
            self.sum_pmin[hour] += hd.pmin
            self.sum_pmin_technological[hour] += hd.pmin_technological
            self.sum_preg[hour] += hd.pmax - hd.pmin_technological
            self.sum_pmax[hour] += hd.pmax

    def get_sum_pmin(self, hour):
        return sum(map(lambda dgu: dgu.hour_data[hour].pmin, self.dgus))

    def get_sum_pmin_technological(self, hour):
        return sum(map(lambda dgu: dgu.hour_data[hour].pmin_technological, self.dgus))

    def get_sum_preg(self, hour):
        return sum(map(lambda dgu: dgu.hour_data[hour].pmax - dgu.hour_data[hour].pmin_technological, self.dgus))

    def get_sum_pmax(self, hour):
        return sum(map(lambda dgu: dgu.hour_data[hour].pmax, self.dgus))

    def set_station(self, station):
        self.station = station

    def get_fixedgen_data(self):
        if not self.prepared_fixedgen_data:
            self.prepare_fixedgen_data()
        return self.prepared_fixedgen_data

    def attach_to_fed_station(self, dpg_list):
        if self.fed_station_id:
            dpg_list[self.fed_station_id].add_supply_dpg(self)

    @staticmethod
    def set_wsum_data(wsumgen):
        DpgSupply.wsumgen_data = wsumgen
        DpgSupply.ws_index = {}
        for i, w in enumerate(wsumgen):
            DpgSupply.ws_index[w[ws['rge_code']]] = i

    @staticmethod
    def get_wsumgen_data():
        return DpgSupply.wsumgen_data

    @staticmethod
    def check_volume(volume):
        return volume > 0

    @staticmethod
    def set_last_hour_data(generators_last_hour):
        DpgSupply.last_hour_data = generators_last_hour

    @staticmethod
    def get_last_hour_data():
        return DpgSupply.last_hour_data

    def prepare_fixedgen_data(self):
        if self.is_unpriced_zone:
            return
        if self.is_blocked | self.is_gaes:
            for dgu in self.dgu_list:
                for hour, hd in dgu.dgu_hour_data.items():
                    if hd.p:
                        if dgu.node.get_node_hour_state(hour):
                            volume = hd.p * (-1 if dgu.node.get_node_hour_type(hour) == 1 else 1)
                            self.prepared_fixedgen_data.append((
                                hour, dgu.code, dgu.node.node_code, self.code, volume
                            ))

    def distribute_bid(self):
        for dgu in self.dgus:
            for gu in dgu.gus:
                if not gu.fuel_types:
                    continue
                if PINTSCHGASTFUEL in list(map(int, re.split(',', gu.fuel_types))):
                    self.is_pintsch_gas = True
        if self.is_unpriced_zone or self.is_blocked or self.is_gaes:
            return
        for dgu in self.dgus:
            for hd in dgu.hour_data:
                # -20 ступень
                if dgu.wsumgen:
                    volume = hd.pmin
                else:
                    volume = max(hd.pmin_agg, hd.pmin_so,
                                  hd.pmin if self.station.type == HYDROSTATIONTYPE else 0)
                if self.check_volume(volume):
                    try:
                        self.distributed_bid.append((
                            hd.hour, dgu.node.code, volume, volume,
                            PMINTECHPRICE, dgu.code, PMINTECHINTERVAL, 0, TARIFF, FORCEDSMOOTH
                        ))
                    except Exception:
                        print('ERROR! DGU %i has no node! (DPG %s)' % (dgu.code, self.code))
                # -18 ступень
                volume = hd.pmin - volume
                if self.check_volume(volume):
                    self.distributed_bid.append((
                        hd.hour, dgu.node.code, volume, volume,
                        PMINPRICE, dgu.code, PMININTERVAL, 0, TARIFF, FORCEDSMOOTH
                    ))
                # интегральная ступень
                if dgu.wsumgen:
                    if dgu.wsumgen.hour_start <= hd.hour <= dgu.wsumgen.hour_end:
                        volume = hd.pmax - hd.pmin
                        if self.check_volume(volume):
                            self.distributed_bid.append((
                                hd.hour, dgu.node.code, volume, 0, dgu.wsumgen.price,
                                dgu.code, HYDROINTERVAL, dgu.wsumgen.integral_id, TARIFF, FORCEDSMOOTH
                            ))
                elif self.station.type == HYDROSTATIONTYPE:
                    volume = min(hd.p, hd.pmax) - hd.pmin
                    if self.check_volume(volume):
                        self.distributed_bid.append((
                            hd.hour, dgu.node.code, volume, volume, PMINPRICE,
                            dgu.code, HYDROINTERVAL, 0, TARIFF, FORCEDSMOOTH
                        ))
                else:
                    if not self.bid:
                        continue
                    if not self.bid.hour_data[hd.hour]:
                        continue
                    prev_volume = hd.pmin
                    sum_pmax = self.get_sum_pmax(hd.hour)
                    sum_pmin = self.get_sum_pmin(hd.hour)
                    sum_pmin_technological = self.get_sum_pmin_technological(hd.hour)
                    sum_preg = self.get_sum_preg(hd.hour)

                    for bid in self.bid.hour_data[hd.hour].interval_data:
                        if sum_pmax > sum_pmin:
                            if min(bid.volume, sum_pmax) <= sum_pmin:
                                k_distr = (hd.pmin_technological / sum_pmin_technological) if sum_pmin_technological else 0
                            else:
                                k_distr = ((hd.pmax - hd.pmin_technological) / sum_preg) if sum_preg else 0
                        else:
                            k_distr = (hd.pmax / sum_pmax) if sum_pmax else 0
                        bid_dgu = (bid.volume - sum_pmin) * k_distr + hd.pmin
                        volume = min(bid_dgu, hd.pmax) - prev_volume
                        if self.check_volume(volume):
                            prev_volume = bid_dgu
                            min_volume = volume if bid.interval_number < 0 else 0
                            price_acc = PMINPRICE if self.is_pintsch_gas else PRICEACC
                            price = bid.price if bid.price else price_acc
                            self.distributed_bid.append((
                                hd.hour, dgu.node.code, volume, min_volume,
                                price, dgu.code, bid.interval_number, 0, TARIFF, FORCEDSMOOTH
                            ))
