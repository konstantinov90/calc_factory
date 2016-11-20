import itertools
import traceback
from operator import itemgetter
from sql_scripts import WsumgenScript
from sql_scripts import BidPairsScript
from sql_scripts import GeneratorsScript
from eq_db.classes.dpg import Dpg

gs = GeneratorsScript()
ws = WsumgenScript()
bps = BidPairsScript()


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


class DpgSupply(Dpg):
    def __init__(self, gs_row):
        super().__init__()
        self.code = gs_row[gs['dpg_code']]
        self.id = gs_row[gs['gtp_id']]
        self.is_gaes = gs_row[gs['is_gaes']]
        self.is_blocked = gs_row[gs['is_blocked']]
        self.is_pintsch_gas = False
        self.is_unpriced_zone = gs_row[gs['is_unpriced_zone']]
        self.fed_station_id = gs_row[gs['fed_station_id']]
        self.station_id = gs_row[gs['station_id']]
        self.dgu_list = []
        self.station = None
        self.prepared_fixedgen_data = []
        self.sum_pmin = [0 for hour in range(HOURCOUNT)]
        self.sum_pmin_technological = [0 for hour in range(HOURCOUNT)]
        self.sum_preg = [0 for hour in range(HOURCOUNT)]
        self.sum_pmax = [0 for hour in range(HOURCOUNT)]

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
        for dgu in self.dgu_list:
            for gu in dgu.gu_list:
                if PINTSCHGASTFUEL in gu.fuel_type_list:
                    self.is_pintsch_gas = True
        if self.is_unpriced_zone \
           | self.is_blocked \
           | self.is_gaes:
            return
        for dgu in self.dgu_list:

            if dgu.code in self.ws_index:
                w = self.wsumgen_data[self.ws_index[dgu.code]]
                do_integral = True
            else:
                do_integral = False

            for hour, hd in dgu.dgu_hour_data.items():
                # -20 ступень
                if do_integral:
                    volume = hd.pmin
                else:
                    volume = max(hd.pmin_agg, hd.pmin_so,
                                  hd.pmin if self.station.type == HYDROSTATIONTYPE else 0)
                if self.check_volume(volume):
                    try:
                        self.distributed_bid.append((
                            hour, dgu.node.node_code, volume, volume,
                            PMINTECHPRICE, dgu.code, PMINTECHINTERVAL, 0, TARIFF, FORCEDSMOOTH
                        ))
                    except Exception:
                        print('ERROR! DGU %i has no node! (DPG %s)' % (dgu.code, self.code))
                # -18 ступень
                volume = hd.pmin - volume
                if self.check_volume(volume):
                    self.distributed_bid.append((
                        hour, dgu.node.node_code, volume, volume,
                        PMINPRICE, dgu.code, PMININTERVAL, 0, TARIFF, FORCEDSMOOTH
                    ))
                # интегральная ступень
                if do_integral:
                    if w[ws['hour_start']] <= hour <= w[ws['hour_end']]:
                        volume = hd.pmax - hd.pmin
                        if self.check_volume(volume):
                            self.distributed_bid.append((
                                hour, dgu.node.node_code, volume, 0, w[ws['price']],
                                dgu.code, HYDROINTERVAL, w[ws['integral_id']], TARIFF, FORCEDSMOOTH
                            ))
                elif self.station.type == HYDROSTATIONTYPE:
                    volume = min(hd.p, hd.pmax) - hd.pmin
                    if self.check_volume(volume):
                        self.distributed_bid.append((
                            hour, dgu.node.node_code, volume, volume, PMINPRICE,
                            dgu.code, HYDROINTERVAL, 0, TARIFF, FORCEDSMOOTH
                        ))
                else:
                    if not self.bid:
                        continue
                    if not self.bid[hour]:
                        continue
                    prev_volume = hd.pmin
                    for bid in self.bid[hour]:
                        if self.sum_pmax[hour] > self.sum_pmin[hour]:
                            if min(bid[bps['volume']], self.sum_pmax[hour]) <= self.sum_pmin[hour]:
                                k_distr = (hd.pmin_technological / self.sum_pmin_technological[hour]) if self.sum_pmin_technological[hour] else 0
                            else:
                                k_distr = ((hd.pmax - hd.pmin_technological) / self.sum_preg[hour]) if self.sum_preg[hour] else 0
                        else:
                            k_distr = (hd.pmax / self.sum_pmax[hour]) if self.sum_pmax[hour] else 0
                        bid_dgu = (bid[bps['volume']] - self.sum_pmin[hour]) * k_distr + hd.pmin
                        volume = min(bid_dgu, hd.pmax) - prev_volume
                        if self.check_volume(volume):
                            prev_volume = bid_dgu
                            min_volume = volume if bid[bps['interval_number']] < 0 else 0
                            price_acc = PMINPRICE if self.is_pintsch_gas else PRICEACC
                            price = bid[bps['price']] if bid[bps['price']] else price_acc
                            self.distributed_bid.append((
                                hour, dgu.node.node_code, volume, min_volume,
                                price, dgu.code, bid[bps['interval_number']], 0, TARIFF, FORCEDSMOOTH
                            ))
