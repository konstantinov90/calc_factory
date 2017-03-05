"""Class DpgSupply."""
# import re
import constants as C
from .base_dpg import Dpg


class DpgSupply(Dpg):
    """class DpgSupply"""
    def __init__(self, gs_row, is_new=False):
        super().__init__(gs_row.gtp_id, gs_row.dpg_code, gs_row.is_unpriced_zone,
                         gs_row.is_spot_trader, gs_row.region_code, gs_row.price_zone_code,
                         gs_row.participant_id, is_new)
        self.is_gaes = bool(gs_row.is_gaes)
        self.is_blocked = bool(gs_row.is_blocked)
        self.is_pintsch_gas = None
        self.oes_code = gs_row.oes_code
        self.name = gs_row.name
        self.fed_station_id = gs_row.fed_station_id
        self.dpg_demand_id = gs_row.dpg_demand_id
        self.station_id = gs_row.station_id
        self.dgus = []
        self.station = None
        self.fed_station = None
        self.dpg_demand = None
        self.sum_pmax_lst = []
        self.sum_pmin_lst = []
        self.prepared_fixedgen_data = []

    lst = {'id': {}, 'code': {}}
    def _init_on_load(self):
        """additional initialization"""
        super()._init_on_load()
        if self._id not in self.lst['id']:
            self.lst['id'][self._id] = self
        if self.code not in self.lst['code']:
            self.lst['code'][self.code] = self

    def add_dgu(self, dgu):
        """add Dgu instance"""
        self.dgus.append(dgu)

    # def get_sum_pmin(self, hour):
    #     """get sum of Dgu instances' pmin"""
    #     return sum(dgu.hour_data[hour].pmin for dgu in self.dgus)
    #
    # def get_sum_pmin_technological(self, hour):
    #     """get sum of Dgu instances' pmin technological"""
    #     return sum(dgu.hour_data[hour].pmin_technological for dgu in self.dgus)
    #
    # def get_sum_preg(self, hour):
    #     """get sum of Dgu instances' preg - regulation range"""
    #     func = lambda dgu, _hr: dgu.hour_data[_hr].pmax - dgu.hour_data[_hr].pmin_technological
    #     return sum(func(dgu, hour) for dgu in self.dgus)
    #
    # def get_sum_pmax(self, hour):
    #     """get sum of Dgu instances' pmax"""
    #     return sum(dgu.hour_data[hour].pmax for dgu in self.dgus)

    def set_station(self, stations_list):
        """set Station instance"""
        self.station = stations_list[self.station_id]
        if not self.station:
            raise Exception('Dpg %s has station_id %i that does not exist!'
                            % (self.code, self.station_id))
        self.station.add_dpg(self)

    def set_fed_station(self, dpg_list):
        """set fed station DpgDemand instance"""
        if self.fed_station_id:
            self.fed_station = dpg_list.by_id[self.fed_station_id]
            if self.fed_station:
                self.fed_station.add_supply_dpg(self)

    def set_dpg_demand(self, dpg_list):
        """set dpg demand instance"""
        if self.dpg_demand_id:
            self.dpg_demand = dpg_list.by_id[self.dpg_demand_id]
            try:
                self.dpg_demand.add_bs_or_gaes_supply(self)
            except AttributeError:
                print('dpg %s has no corresponding dpg demand %i' % (self.code, self.dpg_demand_id))

    def recalculate(self):
        """additional calculation"""
        for hours in zip(*[dgu.hour_data for dgu in self.dgus]):
            sum_pmax = sum(_hd.pmax for _hd in hours)
            sum_pmin = sum(_hd.pmin for _hd in hours)
            sum_preg = sum(_hd.pmax - _hd.pmin_technological for _hd in hours)
            sum_pmin_technological = sum(_hd.pmin_technological for _hd in hours)
            for _hd in hours:
                _hd.kg_min = (_hd.pmin_technological / sum_pmin_technological) \
                               if sum_pmin_technological else None
                _hd.kg_reg = ((_hd.pmax - _hd.pmin_technological) / sum_preg) \
                               if sum_preg else None
                _hd.kg = (_hd.pmax / sum_pmax) if sum_pmax else None
            self.sum_pmax_lst.append(sum_pmax)
            self.sum_pmin_lst.append(sum_pmin)
        sum_fixed_power = sum(dgu.fixed_power for dgu in self.dgus)
        for dgu in self.dgus:
            dgu.kg_fixed = dgu.fixed_power/sum_fixed_power



    def distribute_bid(self):
        """overriden abstract method"""
        for dgu in self.dgus:
            for unit in dgu.gus:
                if C.PINTSCHGASTFUEL in unit.fuel_type_list:
                    self.is_pintsch_gas = True
                    break
        if self.is_unpriced_zone: # or self.is_gaes or self.is_blocked:
            return

        tariff = 0.8 if self.participant_id in C.forced_smooth_participants else C.TARIFF
        forced_smooth = 1 if self.participant_id in C.forced_smooth_participants else 0
        for dgu in self.dgus:
            for _hd in dgu.hour_data:
                # -20 ступень
                if dgu.wsumgen:
                    volume = _hd.pmin
                else:
                    volume = _hd.p if self.is_blocked or self.is_gaes \
                                else max(_hd.pmin_agg, _hd.pmin_so,
                                         _hd.pmin if self.station.type == C.HYDROSTATIONTYPE else 0)
                if self.check_volume(volume):
                    try:
                        self.distributed_bid.append((
                            _hd.hour, dgu.node.code, volume, volume,
                            C.PMINTECHPRICE, dgu.code, C.PMINTECHINTERVAL, 0, tariff, forced_smooth
                        ))
                    except AttributeError:
                        print('ERROR! DGU %i has no node! (DPG %s)' % (dgu.code, self.code))
                if self.is_blocked or self.is_gaes:
                    continue
                # -18 ступень
                volume = _hd.pmin - volume
                if self.check_volume(volume):
                    self.distributed_bid.append((
                        _hd.hour, dgu.node.code, volume, volume,
                        C.PMINPRICE, dgu.code, C.PMININTERVAL, 0, tariff, forced_smooth
                    ))
                # интегральная ступень
                if dgu.wsumgen:
                    if dgu.wsumgen.hour_start <= _hd.hour <= dgu.wsumgen.hour_end:
                        volume = _hd.pmax - _hd.pmin
                        if self.check_volume(volume):
                            self.distributed_bid.append((
                                _hd.hour, dgu.node.code, volume, 0, dgu.wsumgen.price,
                                dgu.code, C.HYDROINTERVAL, dgu.wsumgen.integral_id,
                                tariff, forced_smooth
                            ))
                elif self.station.type == C.HYDROSTATIONTYPE:
                    volume = min(_hd.p, _hd.pmax) - _hd.pmin
                    if self.check_volume(volume):
                        self.distributed_bid.append((
                            _hd.hour, dgu.node.code, volume, volume, C.PMINPRICE,
                            dgu.code, C.HYDROINTERVAL, 0, tariff, forced_smooth
                        ))
                else:
                    if not self.is_spot_trader:
                        continue
                    if not self.bid or not self.bid[_hd.hour]:
                        print('DpgSupply %s bid mandatory!' % self.code)
                        continue
                    prev_volume = _hd.pmin
                    sum_pmax = self.sum_pmax_lst[_hd.hour]
                    sum_pmin = self.sum_pmin_lst[_hd.hour]
                    bid_factor = max(gu.bid_factor for dgu in self.dgus for gu in dgu.gus)

                    for bid in self.bid[_hd.hour].interval_data:
                        if bid is self.bid[_hd.hour].interval_data[-1]:
                            bid_volume = sum_pmax
                        else:
                            bid_volume = bid.volume

                        if sum_pmax > sum_pmin:
                            if min(bid_volume, sum_pmax) <= sum_pmin:
                                k_distr = _hd.kg_min
                            else:
                                k_distr = _hd.kg_reg
                        else:
                            k_distr = _hd.kg
                        k_distr = k_distr if k_distr else 0
                        bid_dgu = (bid_volume - sum_pmin) * k_distr + _hd.pmin
                        volume = min(bid_dgu, _hd.pmax) - prev_volume
                        if self.check_volume(volume):
                            prev_volume = bid_dgu
                            min_volume = volume if bid.interval_number < 0 else 0
                            price_acc = C.PMINPRICE if self.is_pintsch_gas else C.PRICEACC
                            price = (bid.price * bid_factor) if bid.price else price_acc
                            self.distributed_bid.append((
                                _hd.hour, dgu.node.code, volume, min_volume,
                                price, dgu.code, bid.interval_number, 0, tariff, forced_smooth
                            ))
