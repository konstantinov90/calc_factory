from sqlalchemy import *
from sqlalchemy.orm import reconstructor
from utils.ORM import Base
from .dpg_demand import DpgDemand

from sql_scripts import bid_pair_script as bps
from sql_scripts import consumers_script as cs


class DpgDemandSystem(DpgDemand, Base):
    is_gp = Column(Boolean, nullable=True)

    polymorphic_identity = 'system'
    __mapper_args__ = {
        'polymorphic_identity': polymorphic_identity
    }

    def __init__(self, cs_row):
        super().__init__(cs_row)
        self.is_gp = True if cs_row[cs['is_gp']] else False

    lst = {'id': {}, 'code': {}}
    @reconstructor
    def _init_on_load(self):
        super()._init_on_load()
        if self.id not in self.lst['id']:
            self.lst['id'][self.id] = self
        if self.code not in self.lst['code']:
            self.lst['code'][self.code] = self

    def recalculate(self):
        if not self.area:
            return
        for hd in self.area.hour_data:
            hd.sum_pn_retail_diff = sum(map(lambda node: max(node.hour_data[hd.hour].pn - node.hour_data[hd.hour].retail, 0) if node.hour_data[hd.hour].state else 0, self.area.nodes))
            hd.nodes_on = sum(map(lambda node: 1 if node.get_node_hour_state(hd.hour) else 0, self.area.nodes))
        for node in self.area.nodes:
            for hd in node.hour_data:
                if self.area.hour_data[hd.hour].sum_pn_retail_diff:
                    k_distr = (max(hd.pn - hd.retail, 0) if hd.state else 0) / self.area.hour_data[hd.hour].sum_pn_retail_diff
                elif self.area.hour_data[hd.hour].nodes_on:
                    k_distr = (1 if hd.state else 0) / self.area.hour_data[hd.hour].nodes_on
                else:
                    k_distr = 0
                hd.k_distr = k_distr

    def distribute_bid(self):
        if not self.bid:
            return
        for node in self.area.nodes:
            for hd in node.hour_data:
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
                            volume, price , 0 if bid.price else 1
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
