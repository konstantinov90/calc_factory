"""Class DpgDemandSystem."""
from .dpg_demand import DpgDemand
from ..bids_max_prices import BidMaxPrice

UNPRICED_AREA = ('PCHITAZN', 'PAMUREZN')
UNPRICED_AREA_INTERVAL = -55
FED_STATION_INTERVAL = -52


class DpgDemandSystem(DpgDemand):
    """class DpgDemandSystem"""
    def __init__(self, cs_row):
        super().__init__(cs_row)
        self.is_gp = bool(cs_row.is_gp)

    lst = {'id': {}, 'code': {}}
    def _init_on_load(self):
        """additional initialization"""
        super()._init_on_load()
        if self._id not in self.lst['id']:
            self.lst['id'][self._id] = self
        if self.code not in self.lst['code']:
            self.lst['code'][self.code] = self

    def remove(self):
        """clear instance from class list"""
        super().remove()
        print('system')
        del DpgDemandSystem.lst['id'][self._id]
        del DpgDemandSystem.lst['code'][self.code]

    def recalculate(self):
        """additional recalculation after model initialization"""
        if not self.area:
            return

        func = lambda node, hour: max(node.hour_data[hour].pn - node.hour_data[hour].retail, 0)
        for _hd in self.area.hour_data:

            _hd.sum_pn_retail_diff = \
                sum(func(node, _hd.hour) if node.get_node_hour_state(_hd.hour) else 0 \
                    for node in self.area.nodes)

            _hd.nodes_on = sum(1 if node.get_node_hour_state(_hd.hour) else 0 \
                               for node in self.area.nodes)

        for node in self.area.nodes:
            for _hd in node.hour_data:
                if self.area.hour_data[_hd.hour].sum_pn_retail_diff:
                    k_distr = (max(_hd.pn - _hd.retail, 0) if _hd.state else 0) \
                            / self.area.hour_data[_hd.hour].sum_pn_retail_diff
                elif self.area.hour_data[_hd.hour].nodes_on:
                    k_distr = (1 if _hd.state else 0) / self.area.hour_data[_hd.hour].nodes_on
                else:
                    k_distr = 0
                _hd.k_distr = k_distr

    def _distribute_unpriced_area(self):
        """distribute unpriced area bid"""
        for node in self.area.nodes:
            for node_hd in node.hour_data:
                if self.check_volume(node_hd.pn):
                    self.distributed_bid.append((
                        node_hd.hour, 1, self.consumer_code, UNPRICED_AREA_INTERVAL,
                        node.code, node_hd.pn, BidMaxPrice[node_hd.hour].price * 1e6, 1
                    ))

    def _distribute_fed_station(self):
        """distribute fed station bid"""
        for area_hd in self.area.hour_data:
            hour = area_hd.hour
            p_n = sum(max(node.hour_data[hour].pn, 0) for node in self.area.nodes
                      if node.hour_data[hour].state)
            if self.is_disqualified:
                dds = self.disqualified_data
                if not dds.attached_supplies_gen or not dds.fed_station_cons:
                    volume = 0
                else:
                    p_g = sum(dgu.hour_data[hour].p for dpg_sup in self.supply_dpgs
                              for dgu in dpg_sup.dgus)
                    if p_g:
                        volume = min(dds.coeff * p_g, p_n)
                    else:
                        volume = p_n
            else:
                losses = area_hd.losses
                dpg_hd = self.consumer.hour_data[hour]
                volume = max(dpg_hd.pdem - losses, 0)
            for node in self.area.nodes:
                node_hd = node.hour_data[hour]
                value = max(node_hd.pn, 0) / p_n * volume
                if self.check_volume(value) and node_hd.state:
                    self.distributed_bid.append((
                        hour, 1, self.consumer_code, FED_STATION_INTERVAL,
                        node.code, value, BidMaxPrice[node_hd.hour].price * 1e6, 1
                    ))


    def distribute_bid(self):
        """overriden abstract method"""
        if self.is_fed_station and not self.is_gp and not self.is_unpriced_zone:
            self._distribute_fed_station()
        elif self.code in UNPRICED_AREA:
            self._distribute_unpriced_area()
        elif self.supply_gaes:
            self._distribute_gaes()
        else:
            if not self.bid or not self.is_spot_trader:
                return
            for node in self.area.nodes:
                for _hd in node.hour_data:
                    hour = _hd.hour
                    prev_volume = 0
                    bid_hour = self.bid[hour]
                    if not bid_hour:
                        continue

                    for bid in bid_hour.interval_data:
                        volume = (bid.volume - prev_volume) * _hd.k_distr
                        if self.check_volume(volume):
                            prev_volume = bid.volume
                            interval = bid.interval_number
                            if interval == -15:
                                price = BidMaxPrice[hour].price * 4
                            elif interval == -8:
                                price = BidMaxPrice[hour].price * 3
                            elif bid.price:
                                price = bid.price
                            else:
                                price = BidMaxPrice[hour].price * 2
                            self.distributed_bid.append((
                                hour, 1, self.consumer_code, interval, node.code,
                                volume, price, 0 if bid.price else 1
                            ))

    def fill_db(self, con):
        """fill kc_dpg_node"""
        if not self.is_unpriced_zone or self.code in UNPRICED_AREA:
            try:
                self.area.fill_db(con)
            except AttrbuteError as ae:
                print('%s -> %r' % (self.code, ae))
