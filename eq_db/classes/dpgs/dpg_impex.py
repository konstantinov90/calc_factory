"""Class DpgImpex."""
from .base_dpg import Dpg
from ..bids_max_prices import BidMaxPrice

HOURCOUNT = 24
PMININTERVAL = -7
PMINPRICEACC = 0
PRICEACC = 0.8


class DpgImpex(Dpg):
    """class DpgImpex"""
    def __init__(self, imp_s_row):
        super().__init__(imp_s_row.dpg_id, imp_s_row.dpg_code,
                         imp_s_row.is_unpriced_zone, imp_s_row.is_spot_trader,
                         imp_s_row.region_code)
        self.section_code = int(imp_s_row.section_number)
        self.direction = imp_s_row.direction
        self.section = None

    lst = {'id': {}, 'code': {}}
    def _init_on_load(self):
        """additional initialization"""
        super()._init_on_load()
        if self._id not in self.lst['id']:
            self.lst['id'][self._id] = self
        if self.code not in self.lst['code']:
            self.lst['code'][self.code] = self

    def set_section(self, sections_list):
        """set Section instance"""
        section = sections_list[self.section_code]
        if section:
            self.section = section
            section.add_dpg(self)

    def distribute_bid(self):
        """overriden abstract method"""
        if self.is_unpriced_zone:
            return
        if not self.section.is_optimizable:
            return
        for _hd in self.section.hour_data:
            hour = _hd.hour
            if self.direction == 1:
                prev_volume = max(_hd.p_min, 0)
            elif self.direction == 2:
                prev_volume = -min(_hd.p_max, 0)
            if prev_volume:
                price = BidMaxPrice[hour].price if self.direction == 1 else PMINPRICEACC
                self.distributed_bid.append((
                    _hd.hour, self.section.code, self.direction,
                    PMININTERVAL, prev_volume, price, 1
                ))
            if not self.bid or not self.is_spot_trader:
                continue
            bid_hour = self.bid[_hd.hour]
            if not bid_hour:
                continue
            for bid in bid_hour.interval_data:
                volume = bid.volume - prev_volume
                if volume > 0:
                    prev_volume = bid.volume
                    price_original = bid.price
                    if price_original:
                        price = price_original
                        is_price_acceptance = 0
                    else:
                        price = BidMaxPrice[hour].price if self.direction == 1 else PRICEACC
                        is_price_acceptance = 1
                    self.distributed_bid.append((
                        _hd.hour, self.section.code, self.direction,
                        bid.interval_number, volume, price, is_price_acceptance
                    ))
