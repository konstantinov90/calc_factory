"""Class ImpexArea."""
import itertools
import constants
from ..meta_base import MetaBase


class ImpexArea(object, metaclass=MetaBase):
    """class ImpexArea"""
    seq = itertools.count()
    def __init__(self, ias_row):
        self._id = next(self.seq)
        self.section_code, self.area_code, *_ = ias_row
        self.is_europe = True if ias_row.is_europe else False
        self.is_optimizable = True if ias_row.optimized else False
        self._init_on_load()

    def _init_on_load(self):
        """additional initialization"""
        if self._id not in self.lst.keys():
            self.lst[self._id] = self

    def __repr__(self):
        return '<ImpexArea %r: %r>' % (self.section_code, self.area_code)

    def get_impex_area_data(self):
        """get eq_db_impex_area view data"""
        if self.section_code == constants.section_nkaz:
            price_zone = 0
        elif self.is_europe:
            price_zone = 1
        else:
            price_zone = 2
        return (self.section_code, self.area_code, price_zone)
