"""Abstract class DpgDemand."""
from .base_dpg import Dpg
from .dpg_disqualified import DpgDisqualified
from ..bids_max_prices import BidMaxPrice

HOURCOUNT = 24
UNPRICED_AREA = ('PCHITAZN', 'PAMUREZN')
GAES_INTERVAL = -32


class DpgDemand(Dpg):
    """abstract class DpgDemand"""
    def __init__(self, cs_row, is_new=False):
        super().__init__(cs_row.dpg_id, cs_row.dpg_code, cs_row.is_unpriced_zone,
                         cs_row.is_spot_trader, cs_row.region_code, cs_row.price_zone_code,
                         cs_row.participant_id, is_new)
        self.area_external, self.consumer_code, self.area_code, self.dem_rep_volume, \
        self.dem_rep_hours, *_ = cs_row
        self.is_system = bool(cs_row.is_system)
        self.is_fed_station = bool(cs_row.is_fed_station)
        self.is_disqualified = bool(cs_row.is_disqualified)
        self.is_fsk = bool(cs_row.is_fsk)
        self.is_gp = bool(cs_row.is_gp)
        self.min_forecast = None if cs_row.min_forecast is None else (cs_row.min_forecast / 1000)
        self.max_forecast = 1000000 if cs_row.max_forecast is None else (cs_row.max_forecast / 1000)
        self.supply_dpgs = []
        self.block_stations = []
        self.prepared_fixedcon_data = []
        self.demand_response_data = []
        self.supply_gaes = None
        self.disqualified_data = None
        self.consumer = None
        self.load = None
        self.area = None

    lst = {'id': {}, 'code': {}}
    def _init_on_load(self):
        """additional initialization"""
        super()._init_on_load()
        if self._id not in DpgDemand.lst['id']:
            DpgDemand.lst['id'][self._id] = self
        if self.code not in DpgDemand.lst['code']:
            DpgDemand.lst['code'][self.code] = self

    def remove(self):
        """clear instance from class list"""
        super().remove()
        del DpgDemand.lst['id'][self._id]
        del DpgDemand.lst['code'][self.code]

    def get_con_value(self, hour, price_zone_code):
        """get instance consumption value"""
        if price_zone_code == 1:
            return sum(b[5] for b in self.get_distributed_bid() \
                        if b[0] == hour and b[4] <= 999999)
        elif price_zone_code == 2:
            return sum(b[5] for b in self.get_distributed_bid() \
                        if b[0] == hour and b[4] > 999999)
        else:
            raise Exception('Wrong price zone code!')

    def set_consumer(self, consumers_list):
        """set Consumer instance"""
        self.consumer = consumers_list[self.consumer_code]

    def get_pdem(self, hour):
        """get maximum demand value at hour"""
        max_bid = None
        if self.bid:
            if self.bid[hour]:
                if self.bid[hour].interval_data:
                    bid = self.bid[hour].interval_data[-1]
                    max_bid = bid.volume_init
        values = (max_bid, self.max_forecast, self.consumer.hour_data[hour].pdem)
        # print(self.code, hour, values)
        corrected_pdem = min(val for val in values if val is not None)
        # if max_bid:
        #     if corrected_pdem != max_bid:
        #         print('Dpg %s pdem corrected' % self.code)
        return corrected_pdem

    def set_load(self, loads_list):
        """set Load instance"""
        self.load = loads_list[self.consumer_code]
        if self.load:
            self.load.set_dpg(self)

    def set_area(self, areas_list):
        """set Area instance"""
        self.area = areas_list[self.area_code]
        if self.area and (self.is_system or self.is_fsk):
            if self.area.dpg:
                raise Exception('too many system dpgs for area %i' % self.area.code)
            self.area.dpg = self

    def add_supply_dpg(self, supply_dpg):
        """add DpgSupply instance"""
        self.supply_dpgs.append(supply_dpg)

    def add_bs_or_gaes_supply(self, supply_dpg):
        """add block station or gaes instance"""
        if supply_dpg.is_blocked:
            self.block_stations.append(supply_dpg)
        elif supply_dpg.is_gaes:
            self.supply_gaes = supply_dpg
        else:
            raise Exception('%s: BS or GAES dpg %s error!' % (self.code, supply_dpg.code))

    def add_disqualified_data(self, dds_row):
        """set DpgDisqualified instance"""
        if self.disqualified_data:
            raise Exception('cannot self DpgDemand.disqualified_data twice!')
        self.disqualified_data = DpgDisqualified(dds_row)

    def _distribute_gaes(self):
        """distribute gaes consumption"""
        for dgu in self.supply_gaes.dgus:
            for _hd in dgu.hour_data:
                volume = -_hd.p
                if self.check_volume(volume):
                    self.distributed_bid.append((
                        _hd.hour, 1, self.consumer_code, GAES_INTERVAL,
                        dgu.node.code, volume, BidMaxPrice[_hd.hour].price * 1e6, 1
                    ))

    def get_demand_response_data(self):
        """get prepared demand response data"""
        if not self.demand_response_data:
            self.prepare_demand_response_data()
        return self.demand_response_data

    def prepare_demand_response_data(self):
        """prepare demand response data"""
        if self.is_unpriced_zone or self.supply_gaes:
            return
        if self.consumer.dem_rep_ready and self.dem_rep_volume:
            self.demand_response_data.append((
                self.consumer.code, self.dem_rep_volume, self.dem_rep_hours
            ))
