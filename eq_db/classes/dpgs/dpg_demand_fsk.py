from sqlalchemy.orm import reconstructor
from utils.ORM import Base
from .dpg_demand import DpgDemand


class DpgDemandFSK(DpgDemand, Base):
    polymorphic_identity = 'FSK'
    __mapper_args__ = {
        'polymorphic_identity': polymorphic_identity
    }

    def __init__(self, cs_row):
        super().__init__(cs_row)
        self.area_obj = None

    lst = {'id': {}, 'code': {}}
    @reconstructor
    def _init_on_load(self):
        super()._init_on_load()
        if self.id not in self.lst['id']:
            self.lst['id'][self.id] = self
        if self.code not in self.lst['code']:
            self.lst['code'][self.code] = self

    def prepare_fixedcon_data(self):
        if self.is_unpriced_zone or not self.area_obj:
            return
        for node in self.area_obj.nodes:
            for hour, node_data in node.hour_data.items():
                if node_data.pn > 0:
                    if node_data.type == 0:
                        sign = 0
                    elif node_data.type != 1:
                        sign = -1
                    else:
                        sign = 1
                    self.prepared_fixedcon_data.append((
                        hour, node.node_code, self.code, node_data.pn * sign
                    ))
