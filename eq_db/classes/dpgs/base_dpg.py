"""Base abstract class Dpg."""
import abc
from utils.subscriptable import subscriptable
from ..meta_base import MetaBase


class Dpg(object, metaclass=MetaBase):
    """base abstract class Dpg"""

    lst = {'id': {}, 'code': {}}
    def __init__(self, _id, code, is_unpriced_zone, is_spot_trader, region_code):
        self._id = _id
        self.code = code
        self.is_unpriced_zone = is_unpriced_zone
        self.is_spot_trader = True if is_spot_trader else False
        self.region_code = region_code
        self.bid = None
        self.distributed_bid = []
        self._init_on_load()

    def _init_on_load(self):
        """additional initialization"""
        if self._id not in Dpg.lst['id']:
            Dpg.lst['id'][self._id] = self
        if self.code not in Dpg.lst['code']:
            Dpg.lst['code'][self.code] = self

    @subscriptable
    @staticmethod
    def by_id(item):
        """get Dpg instance by id"""
        return Dpg['id', item]

    @subscriptable
    @staticmethod
    def by_code(item):
        """get Dpg instance by code"""
        return Dpg['code', item]

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.code)

    @abc.abstractmethod
    def distribute_bid(self):
        """distribute bid abstract method"""
        pass

    def get_distributed_bid(self):
        """get eq_db_demands/supplies view data"""
        if not self.distributed_bid:
            self.distribute_bid()
        return self.distributed_bid

    def set_bid(self, bids_list):
        """set Bid instance"""
        # bid = bids_list.by_dpg_id[self.code]
        # if not bid:
        bid = bids_list[self._id]
        self.bid = bid

    @staticmethod
    def check_volume(volume):
        """check volume significant value"""
        return volume > 1e-13
