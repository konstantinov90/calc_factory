from operator import itemgetter
from sql_scripts import BidPairsScript, ImpexDpgsScript


HOURCOUNT = 24
PMININTERVAL = -7
PMINPRICEACC = 0
PRICEACC = 0.8

bps = BidPairsScript()
imp_s = ImpexDpgsScript()


class Dpg(object):
    def __init__(self):
        self.code = ''
        self.bid = None
        self.distributed_bid = []
        self.id = ''
        self.station_id = None

    def set_bid(self, bid):
        self.bid = bid

    def calculate_node_load(self):
        pass

    def calculate_dpg_node_load(self):
        pass

    def __str__(self):
        return self.code

    def get_distributed_bid(self):
        if not self.distributed_bid:
            self.distribute_bid()
        return self.distributed_bid

    @staticmethod
    def set_max_bid_prices(max_prices):
        Dpg.max_bid_prices = {}
        for hour, price in max_prices:
            if hour in Dpg.max_bid_prices.keys():
                raise Exception('too many max prices!')
            Dpg.max_bid_prices[hour] = price

    @staticmethod
    def get_max_bid_price(hour):
        return Dpg.max_bid_prices[hour]

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


class DpgImpex(Dpg):
    def __init__(self, imp_s_row):
        super().__init__()
        self.code = imp_s_row[imp_s['dpg_code']]
        self.id = imp_s_row[imp_s['dpg_id']]
        self.section_number = int(imp_s_row[imp_s['section_number']])
        self.section = None
        self.direction = imp_s_row[imp_s['direction']]
        self.is_unpriced_zone = imp_s_row[imp_s['is_unpriced_zone']]
        self.section_impex_data = []

    def attach_sections(self, sections_list):
        section = sections_list[self.section_number]
        if section:
            self.section = section
            section.add_dpg(self)

    def distribute_bid(self):
        if self.is_unpriced_zone:
            return
        if not self.section.is_optimizable():
            return
        for hour, sh in enumerate(self.section.hour_data):
            if self.direction == 1:
                prev_volume = max(sh.p_min, 0)
            elif self.direction == 2:
                prev_volume = -min(sh.p_max, 0)
            if prev_volume:
                price = self.get_max_bid_price(hour) if self.direction == 1 else PMINPRICEACC
                self.distributed_bid.append((
                    hour, self.section_number, self.direction, PMININTERVAL, prev_volume, price, 1
                ))
            if not self.bid:
                continue
            if not self.bid[hour]:
                continue
            for bid in self.bid[hour]:
                volume = bid[bps['volume']] - prev_volume
                if volume > 0:
                    prev_volume = bid[bps['volume']]
                    price_original = bid[bps['price']]
                    if price_original:
                        price = price_original
                        is_price_acceptance = 0
                    else:
                        price = self.get_max_bid_price(hour) if self.direction == 1 else PRICEACC
                        is_price_acceptance = 1
                    self.distributed_bid.append((
                        hour, self.section_number, self.direction, bid[bps['interval_number']],
                        volume, price, is_price_acceptance
                    ))
