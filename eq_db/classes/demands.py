from time import time
from operator import itemgetter
from sql_scripts import BidPairsScript
# from sql_scripts import KcNodeScript
from sql_scripts import ConsumersScript
from sql_scripts import RastrLoadScript
from sql_scripts import RastrConsumerScript
from sql_scripts import MaxBidPriceScript
from sql_scripts import RastrGenScript
from sql_scripts import DisqualifiedDataScript
from sql_scripts import RastrAreaScript
from eq_db.classes.dpg import Dpg
from utils.progress_bar import update_progress

HOURCOUNT = 24
UNPRICED_AREA = ('PCHITAZN', 'PAMUREZN')

# ks = KcNodeScript()
bps = BidPairsScript()
cs = ConsumersScript()
ls = RastrLoadScript()
rs = RastrConsumerScript()
rg = RastrGenScript()
ds = DisqualifiedDataScript()
ra = RastrAreaScript()


class DpgDemand(Dpg):
    #  static data
    disqualified_data = []

    def __init__(self, cs_row):
        super().__init__()
        self.code = cs_row[cs['dpg_code']]
        self.id = cs_row[cs['dpg_id']]
        self.consumer_code = cs_row[cs['consumer_code']]
        self.is_system = cs_row[cs['is_system']]
        self.is_fed_station = cs_row[cs['is_fed_station']]
        self.is_disqualified = cs_row[cs['is_disqualified']]
        self.is_unpriced_zone = cs_row[cs['is_unpriced_zone']]
        self.area = cs_row[cs['area']]
        self.supply_dpgs = []
        self.prepared_fixedcon_data = []
        self.consumer_obj = None
        self.load_obj = None
        self.area_obj = None
        self.min_forecast = None if cs_row[cs['min_forecast']] is None else (cs_row[cs['min_forecast']] / 1000)
        self.max_forecast = 1000000 if cs_row[cs['max_forecast']] is None else (cs_row[cs['max_forecast']] / 1000)
        self.p_dem = [0 for hour in range(HOURCOUNT)]

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

    def set_load(self, loads_list):
        self.load_obj = loads_list[self.consumer_code]

    def set_area(self, areas_list):
        self.area_obj = areas_list[self.area]

    def add_supply_dpg(self, supply_dpg):
        self.supply_dpgs.append(supply_dpg)

    def get_fixedcon_data(self):
        if not self.prepared_fixedcon_data:
            self.prepare_fixedcon_data()
        return self.prepared_fixedcon_data

    @staticmethod
    def add_disqualified_data(data):
        DpgDemand.disqualified_data = data

    @staticmethod
    def get_disqualified_data():
        return DpgDemand.disqualified_data

    def prepare_fixedcon_data(self):
        pass


class DpgDemandSystem(DpgDemand):
    def __init__(self, cs_row):
        super().__init__(cs_row)
        self.is_gp = cs_row[cs['is_gp']]

    def distribute_bid(self):
        if not self.bid:
            return
        for node in self.area_obj.nodes:
            for hour, hd in node.hour_data.items():
                prev_volume = 0
                if not self.bid[hour]:
                    continue

                for b in self.bid[hour]:
                    volume = (b[bps['volume']] - prev_volume) * hd.k_distr
                    if volume > 0:
                        prev_volume = b[bps['volume']]
                        interval = b[bps['interval_number']]
                        if interval == -15:
                            price = self.get_max_bid_price(hour) * 3
                        elif interval == -8:
                            price = self.get_max_bid_price(hour) * 2
                        elif b[bps['price']]:
                            price = b[bps['price']]
                        else:
                            price = self.get_max_bid_price(hour)
                        self.distributed_bid.append((
                            hour, self.consumer_code, interval, node.node_code,
                            volume, price , 0 if b[bps['price']] else 1
                        ))

    def prepare_fixedcon_data(self):
        if (self.is_unpriced_zone and (self.code not in UNPRICED_AREA)) \
                or self.is_gp:
            return
        if self.is_fed_station or (self.code in UNPRICED_AREA):
            for hour, hd in self.consumer_obj.consumer_hour_data.items():
                p_n = sum([max(node.get_node_hour_load(hour),0) for node in self.area_obj.nodes if node.get_node_hour_state(hour)])
                if not p_n:
                    return
                if self.is_disqualified:
                    dd = [d for d in self.get_disqualified_data() if d[ds['dpg_id']] == self.id]
                    if len(dd) != 1:
                        raise Exception('Wrong disqualified data!!!')
                    if not dd[0][ds['attached_supplies_gen']]:
                        volume = p_n
                    elif not dd[0][ds['fed_station_cons']]:
                        volume = p_n
                    else:
                        coeff = dd[0][ds['fed_station_cons']] / dd[0][ds['attached_supplies_gen']]

                        p_g = 0
                        for d in self.supply_dpgs:
                            for dgu in d.dgu_list:
                                p_g += dgu.dgu_hour_data[hour].p
                        if p_g:
                            volume = min(coeff * p_g, p_n)
                        else:
                            volume = p_n
                else:
                    losses = self.area_obj.area_hour_data[hour].losses
                    volume = max(hd.pdem - losses, 0)
                for node in self.area_obj.nodes:
                    if node.get_node_hour_state(hour):
                        node_type = node.get_node_hour_type(hour)
                        if node_type == 0:
                            sign = 0
                        elif node_type != 1:
                            sign = -1
                        else:
                            sign = 1
                        if self.code in UNPRICED_AREA:
                            value = max(node.get_node_hour_load(hour),0)
                        else:
                            value = max(node.get_node_hour_load(hour),0) / p_n * volume * sign
                        if value:
                            self.prepared_fixedcon_data.append((
                                hour, node.node_code, self.code, value
                            ))


class DpgDemandLoad(DpgDemand):
    def __init__(self, cs_row):
        super().__init__(cs_row)

    def distribute_bid(self):
        if not self.bid:
            return
        for node_data in self.load_obj.nodes.values():
            node = node_data['node_obj']
            for hour, hd in node_data['hour_data'].items():
                prev_volume = 0
                if not self.bid[hour]:
                    continue

                for b in self.bid[hour]:
                    volume = (b[bps['volume']] - prev_volume) * hd.k_distr
                    if volume > 0:
                        prev_volume = b[bps['volume']]
                        interval = b[bps['interval_number']]
                        if interval == -15:
                            price = self.get_max_bid_price(hour) * 3
                        elif interval == -8:
                            price = self.get_max_bid_price(hour) * 2
                        elif b[bps['price']]:
                            price = b[bps['price']]
                        else:
                            price = self.get_max_bid_price(hour)
                        self.distributed_bid.append((
                            hour, self.consumer_code, interval, node.node_code,
                            volume, price , 0 if b[bps['price']] else 1
                        ))

    def prepare_fixedcon_data(self):
        if self.is_unpriced_zone or (not self.is_fed_station):
            return
        for hour, hd in self.consumer_obj.consumer_hour_data.items():
            p_n = self.load_obj.sum_pn[hour]
            if self.is_disqualified:
                dd = [d for d in self.get_disqualified_data() if d[ds['dpg_id']] == self.id]
                if len(dd) != 1:
                    raise Exception('Wrong disqualified data!!!')
                coeff = dd[0][ds['fed_station_cons']] / dd[0][ds['attached_supplies_gen']]

                p_g = 0
                for d in self.supply_dpgs:
                    for dgu in d.dgu_list:
                        p_g += dgu.dgu_hour_data[hour].p
                volume = min(coeff * p_g, p_n)
            else:
                if min(0.25 * p_n, p_n - 110) <= hd.pdem <= max(1.5 * p_n, p_n + 110):
                    volume = hd.pdem
                else:
                    volume = p_n
            for node_code, node_data in self.load_obj.nodes.items():
                node_type = node_data['node_obj'].get_node_hour_type(hour)
                if node_type == 0:
                    sign = 0
                elif node_type != 1:
                    sign = -1
                else:
                    sign = 1
                value = node_data['hour_data'][hour].node_dose / 100 * volume * sign
                if value:
                    self.prepared_fixedcon_data.append((
                        hour, node_code, self.code, value
                    ))

    def calculate_node_load(self):
        if not self.load_obj:
            return
        for node_data in self.load_obj.nodes.values():
            node = node_data['node_obj']
            for hour, hd in node_data['hour_data'].items():
                node.add_to_pdem(hour, self.p_dem[hour] * hd.k_distr)

    def calculate_dpg_node_load(self):
        if not self.load_obj:
            return
        for node_data in self.load_obj.nodes.values():
            node = node_data['node_obj']
            for hour, hd in node_data['hour_data'].items():
                hd.set_pn_dpg_node_share(node.hour_data[hour].pn * ((self.p_dem[hour] * hd.k_distr / node.hour_data[hour].pdem) if node.hour_data[hour].pdem else 0))
                hd.set_pdem_dpg_node_share(node.hour_data[hour].pdem * ((self.p_dem[hour] * hd.k_distr / node.hour_data[hour].pdem) if node.hour_data[hour].pdem else 0))
                node.add_to_retail(hour, hd.pdem_dpg_node_share)


class DpgDemandFSK(DpgDemand):
    def __init__(self, cs_row):
        super().__init__(cs_row)
        self.area_obj = None

    def prepare_fixedcon_data(self):
        if self.is_unpriced_zone or not self.area_obj:
            return
        for node in self.area_obj.nodes:
            for hour, node_data in node.hour_data.items():
                if node_data.pn > 0:
                    if node_data.type == 0:
                        sign = 0
                    elif node_data.type != 1:
                        sign = -1
                    else:
                        sign = 1
                    self.prepared_fixedcon_data.append((
                        hour, node.node_code, self.code, node_data.pn * sign
                    ))
