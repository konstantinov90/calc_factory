"""class DguHourData."""
from collections import namedtuple
from operator import attrgetter

FIELDS_TO_INSERT = '''hour kg
                     p pmax pmin pmin_agg pmin_so kg_min kg_reg
                     pmin_heat pmin_tech'''
KGDPGRGE = namedtuple('kg_dpg_rge_insert', FIELDS_TO_INSERT)


class DguHourData(object):
    """class DguHourData"""
    def __init__(self, rgs_row, parent):
        self.dgu = parent
        self.hour, self.dgu_code, self.pmin, self.pmax, self.pmin_agg, self.pmax_agg, \
            self.pmin_tech, self.pmax_tech, self.pmin_heat, self.pmax_heat, self.pmin_so, \
            self.pmax_so, self.p, self.wmax, self.wmin, self.vgain, self.vdrop, _ = rgs_row
        self.kg = None
        self.kg_min = None
        self.kg_reg = None

    @property
    def pmin_technological(self):
        """pmin_technological attribute"""
        return max(self.pmin, self.pmin_heat, self.pmin_tech)

    def __repr__(self):
        return '<DguHourData: %i>' % self.hour

    def get_insert_data(self):
        """get tuple to insert data to DB"""
        return KGDPGRGE(*attrgetter(*FIELDS_TO_INSERT.split())(self))

    def augment(self, _hd):
        """augment DguHourData by GuHourData"""
        self.pmin += _hd.pmin_t
        self.pmin_agg += _hd.pmin_t
        self.pmin_tech += _hd.pmin_t
        self.pmin_heat += _hd.pmin_t
        self.pmin_so += _hd.pmin_t

        self.p += _hd.pmax_t

        self.pmax += _hd.pmax_t
        self.pmax_agg += _hd.pmax_t
        self.pmax_tech += _hd.pmax_t
        self.pmax_heat += _hd.pmax_t
        self.pmax_so += _hd.pmax_t

        self.vdrop += _hd.vdrop
        self.vgain += _hd.vgain

    def deplete(self, _hd):
        """deplete DguHourData by GuHourData"""
        # if _hd.state:
        self.pmin -= min(_hd.pmax, self.pmin)
        self.pmin_agg -= min(_hd.pmax, self.pmin_agg)
        self.pmin_tech -= min(_hd.pmax, self.pmin_tech)
        self.pmin_heat -= min(_hd.pmax, self.pmin_heat)
        self.pmin_so -= min(_hd.pmax, self.pmin_so)

        self.p -= min(_hd.pmax, self.p)

        self.pmax -= min(_hd.pmax, self.pmax)
        self.pmax_agg -= min(_hd.pmax, self.pmax_agg)
        self.pmax_tech -= min(_hd.pmax, self.pmax_tech)
        self.pmax_heat -= min(_hd.pmax, self.pmax_heat)
        self.pmax_so -= min(_hd.pmax, self.pmax_so)

        self.vdrop -= min(_hd.vdrop, self.vdrop)
        self.vgain -= min(_hd.vgain, self.vgain)

    def turn_off(self):
        """turn off DguHourData"""
        self.pmin = 0
        self.pmax = 0
        self.pmin_agg = 0
        self.pmax_agg = 0
        self.pmin_tech = 0
        self.pmax_tech = 0
        self.pmin_heat = 0
        self.pmax_heat = 0
        self.pmin_so = 0
        self.pmax_so = 0
        self.p = 0
        self.wmax = 0
        self.wmin = 0
        self.vgain = 0
        self.vdrop = 0

        # node_hd = self.dgu.node.hour_data[self.hour]
        # if not sum(dgu.hour_data[self.hour].pmax for dgu in self.dgu.node.dgus) \
        #    and not node_hd.pn:
        #     node_hd.turn_off()
