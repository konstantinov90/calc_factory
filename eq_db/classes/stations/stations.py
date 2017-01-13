"""Station class module."""
from ..meta_base import MetaBase


class Station(object, metaclass=MetaBase):
    """Station class"""
    def __init__(self, ss_row):
        self._id, self.code, self.type, self.category = ss_row
        self.dpgs = []
        self._init_on_load()

    def __repr__(self):
        return '<Station %s>' % self.code

    def _init_on_load(self):
        """initialize instances list"""
        if self._id not in self.lst.keys():
            self.lst[self._id] = self

    def add_dpg(self, dpg):
        """append Dpg to Station instance"""
        self.dpgs.append(dpg)
