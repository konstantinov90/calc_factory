"""Class DpgDemandLoad"""
from .dpg_demand import DpgDemand
from ..bids_max_prices import BidMaxPrice

FED_STATION_INTERVAL = -42

class DpgDemandLoad(DpgDemand):
    """class DpgDemandLoad"""

    lst = {'id': {}, 'code': {}}
    def _init_on_load(self):
        """additional initialization"""
        super()._init_on_load()
        if self._id not in self.lst['id']:
            self.lst['id'][self._id] = self
        if self.code not in self.lst['code']:
            self.lst['code'][self.code] = self

    def _distribute_fed_station(self):
        """distribute fed station bid"""
        for load_hd, consumer_hd in zip(self.load.hour_data, self.consumer.hour_data):
            if load_hd.hour != consumer_hd.hour:
                raise Exception('dpg %s data error!' % self.code)
            hour = load_hd.hour
            p_n = load_hd.pn
            p_dem = consumer_hd.pdem
            if self.is_disqualified:
                p_g = sum(dgu.hour_data[hour].p for dpg_sup in self.supply_dpgs
                          for dgu in dpg_sup.dgus)
                if p_g:
                    volume = min(self.disqualified_data.coeff * p_g, p_n)
                else:
                    volume = p_n
            elif min(0.25 * p_n, p_n - 110) <= p_dem <= max(1.5 * p_n, p_n + 110):
                volume = p_dem
            else:
                volume = p_n
            for _ln in self.load.nodes_data:
                if _ln.hour_data[hour].hour != hour:
                    raise Exception('dpg %s data error!' % self.code)
                value = _ln.hour_data[hour].node_dose / 100 * volume
                if self.check_volume(value):
                    self.distributed_bid.append((
                        hour, 1, self.consumer_code, FED_STATION_INTERVAL,
                        _ln.node.code, value, BidMaxPrice[hour].price * 1e6, 1
                    ))


    def distribute_bid(self):
        """overriden abstract method"""
        if self.is_fed_station and not self.is_unpriced_zone:
            self._distribute_fed_station()
        elif self.supply_gaes:
            self._distribute_gaes()
        else:
            if not self.bid or not self.is_spot_trader:
                return
            for node_data in self.load.nodes_data:
                node = node_data.node
                for _hd in node_data.hour_data:
                    hour = _hd.hour
                    prev_volume = 0
                    bid_hour = self.bid[hour]
                    if not bid_hour:
                        continue

                    for bid in bid_hour.interval_data:
                        volume = (bid.volume - prev_volume) * _hd.k_distr
                        if volume > 0:
                            prev_volume = bid.volume
                            interval = bid.interval_number
                            if interval == -15:
                                price = BidMaxPrice[hour].price * 3
                            elif interval == -8:
                                price = BidMaxPrice[hour].price * 2
                            elif bid.price:
                                price = bid.price
                            else:
                                price = BidMaxPrice[hour].price
                            self.distributed_bid.append((
                                hour, 1, self.consumer_code, interval, node.code,
                                volume, price, 0 if bid.price else 1
                            ))

    def recalculate(self):
        """additional recalculation after model initialization"""
        self._calculate_node_load()
        self._calculate_dpg_node_load()

    def _calculate_node_load(self):
        """add volume to pdem of Node instance"""
        if not self.load:
            return
        for node_data in self.load.nodes_data:
            node = node_data.node
            for _hd in node_data.hour_data:
                node.add_to_pdem(_hd.hour, self.get_pdem(_hd.hour) * _hd.k_distr)

    def _calculate_dpg_node_load(self):
        """calculate pn and pdem share of Node instance"""
        if not self.load:
            return
        for node_data in self.load.nodes_data:
            node = node_data.node
            for _hd in node_data.hour_data:
                node_hour = node.hour_data[_hd.hour]
                corrected_pdem = (self.get_pdem(_hd.hour) * _hd.k_distr / node_hour.pdem) \
                                    if node_hour.pdem else 0
                _hd.set_pn_dpg_node_share(node_hour.pn * corrected_pdem)
                _hd.set_pdem_dpg_node_share(node_hour.pdem * corrected_pdem)
                node.add_to_retail(_hd.hour, _hd.pdem_dpg_node_share)

    def fill_db(self, con):
        """fill kc_dpg_node"""
        if self.supply_gaes:
            script = """INSERT into kc_dpg_node (hour, kc_nodedose, sta, node, dpg_id, is_system,
                                    dpg_code)
                        VALUES (:1, :2, :3, :4, :5, :6, :7)"""
            data = []
            for dgu in self.supply_gaes.dgus:
                for _hd in dgu.hour_data:
                    k_distr = _hd.kg if _hd.kg else dgu.kg_fixed
                    data.append((_hd.hour, k_distr, not dgu.node.get_node_hour_state(_hd.hour),
                                dgu.node.code, self._id, self.is_system, self.code))
            with con.cursor() as curs:
                curs.executemany(script, data)

        elif not self.is_unpriced_zone:
            self.load.fill_db(con)
