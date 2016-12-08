from utils.ORM import Base
from sqlalchemy.orm import reconstructor
from .dpg_demand import DpgDemand

from sql_scripts import bid_pair_script as bps
from sql_scripts import disqualified_data_script as ds


class DpgDemandLoad(DpgDemand, Base):
    polymorphic_identity = 'load'
    __mapper_args__ = {
        'polymorphic_identity': polymorphic_identity
    }

    def __init__(self, cs_row):
        super().__init__(cs_row)

    lst = {'id': {}, 'code': {}}
    @reconstructor
    def _init_on_load(self):
        super()._init_on_load()
        if self.id not in self.lst['id']:
            self.lst['id'][self.id] = self
        if self.code not in self.lst['code']:
            self.lst['code'][self.code] = self
        # self.calculate_node_load()
        # self.calculate_dpg_node_load()

    def distribute_bid(self):
        if not self.bid:
            return
        for node_data in self.load.nodes_data:
            node = node_data.node
            for hd in node_data.hour_data:
                prev_volume = 0
                bid_hour = self.bid.hour_data[hd.hour]
                if not bid_hour:
                    continue

                for bid in bid_hour.interval_data:
                    volume = (bid.volume - prev_volume) * hd.k_distr
                    if volume > 0:
                        prev_volume = bid.volume
                        interval = bid.interval_number
                        if interval == -15:
                            price = bid_hour.max_price.price * 3
                        elif interval == -8:
                            price = bid_hour.max_price.price * 2
                        elif bid.price:
                            price = bid.price
                        else:
                            price = bid_hour.max_price.price
                        self.distributed_bid.append((
                            hd.hour, self.consumer_code, interval, node.code,
                            volume, price, 0 if bid.price else 1
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

    def recalculate(self):
        self._calculate_node_load()
        self._calculate_dpg_node_load()

    def _calculate_node_load(self):
        if not self.load:
            return
        for node_data in self.load.nodes_data:
            node = node_data.node
            for hd in node_data.hour_data:
                node.add_to_pdem(hd.hour, self.get_pdem(hd.hour) * hd.k_distr)

    def _calculate_dpg_node_load(self):
        if not self.load:
            return
        for node_data in self.load.nodes_data:
            node = node_data.node
            for hd in node_data.hour_data:
                hd.set_pn_dpg_node_share(node.hour_data[hd.hour].pn * ((self.get_pdem(hd.hour) * hd.k_distr / node.hour_data[hd.hour].pdem) if node.hour_data[hd.hour].pdem else 0))
                hd.set_pdem_dpg_node_share(node.hour_data[hd.hour].pdem * ((self.get_pdem(hd.hour) * hd.k_distr / node.hour_data[hd.hour].pdem) if node.hour_data[hd.hour].pdem else 0))
                node.add_to_retail(hd.hour, hd.pdem_dpg_node_share)
