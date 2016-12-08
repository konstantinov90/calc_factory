from sqlalchemy import *
from sqlalchemy.orm import relationship, reconstructor

from utils.ORM import Base, MetaBase
from .base_dpg import Dpg
from sql_scripts import impex_dpgs_script as imp_s
from sql_scripts import bid_pair_script as bps

HOURCOUNT = 24
PMININTERVAL = -7
PMINPRICEACC = 0
PRICEACC = 0.8


class DpgImpex(Dpg, Base, metaclass=MetaBase):
    __tablename__ = 'dpg_impex'
    # code inherited
    # id inherited
    # is_unpriced_zone inherited
    section_code = Column(Integer)
    direction = Column(Integer)
    bid = relationship('Bid', foreign_keys='Bid.dpg_id', uselist=False, primaryjoin='Bid.dpg_id == DpgImpex.id')
    section = relationship('Section', primaryjoin='Section.code == DpgImpex.section_code', foreign_keys='Section.code', uselist=False)

    __table_args__ = (
        CheckConstraint('direction in (1, 2)', name='dpg_impex_direction_check'),
    )

    def __init__(self, imp_s_row):
        super().__init__(imp_s_row[imp_s['dpg_id']], imp_s_row[imp_s['dpg_code']], imp_s_row[imp_s['is_unpriced_zone']])
        self.section_code = int(imp_s_row[imp_s['section_number']])
        self.direction = imp_s_row[imp_s['direction']]
        # self.section = None
        # self.section_impex_data = []

    lst = {'id': {}, 'code': {}}
    @reconstructor
    def _init_on_load(self):
        super()._init_on_load()
        if self.id not in self.lst['id']:
            self.lst['id'][self.id] = self
        if self.code not in self.lst['code']:
            self.lst['code'][self.code] = self

    def attach_sections(self, sections_list):
        section = sections_list[self.section_number]
        if section:
            self.section = section
            section.add_dpg(self)

    def distribute_bid(self):
        if self.is_unpriced_zone:
            return
        if not self.section.is_optimizable:
            return
        for hd in self.section.hour_data:
            if self.direction == 1:
                prev_volume = max(hd.p_min, 0)
            elif self.direction == 2:
                prev_volume = -min(hd.p_max, 0)
            if prev_volume:
                price = hd.max_price.price if self.direction == 1 else PMINPRICEACC
                self.distributed_bid.append((
                    hd.hour, self.section.code, self.direction,
                    PMININTERVAL, prev_volume, price, 1
                ))
            if not self.bid:
                continue
            bid_hour = self.bid.hour_data[hd.hour]
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
                        price = bid_hour.max_price.price if self.direction == 1 else PRICEACC
                        is_price_acceptance = 1
                    self.distributed_bid.append((
                        hd.hour, self.section.code, self.direction,
                        bid.interval_number, volume, price, is_price_acceptance
                    ))
