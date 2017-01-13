"""class DguHourData."""


class DguHourData(object):
    """class DguHourData"""
    def __init__(self, rgs_row):
        self.hour, self.dgu_code, self.pmin, self.pmax, self.pmin_agg, self.pmax_agg, \
            self.pmin_tech, self.pmax_tech, self.pmin_heat, self.pmax_heat, self.pmin_so, \
            self.pmax_so, self.p, self.wmax, self.wmin, self.vgain, self.vdrop, _ = rgs_row
        self.pmin_technological = max(self.pmin, self.pmin_heat, self.pmin_tech)

    def __repr__(self):
        return '<DguHourData: %i>' % self.hour

    def deplete(self, _hd):
        '''deplete DguHourData by GuHourData'''
        if _hd.state:
            self.pmin -= min(_hd.pmin, self.pmin)
            self.pmin_agg -= min(_hd.pmin, self.pmin_agg)
            self.pmin_tech -= min(_hd.pmin, self.pmin_tech)
            self.pmin_heat -= min(_hd.pmin, self.pmin_heat)
            self.pmin_so -= min(_hd.pmin, self.pmin_so)

            self.p -= min(_hd.pmax, self.p)

            self.pmax -= min(_hd.pmax, self.pmax)
            self.pmax_agg -= min(_hd.pmax, self.pmax_agg)
            self.pmax_tech -= min(_hd.pmax, self.pmax_tech)
            self.pmax_heat -= min(_hd.pmax, self.pmax_heat)
            self.pmax_so -= min(_hd.pmax, self.pmax_so)

            self.vdrop -= min(_hd.vdrop, self.vdrop)
            self.vgain -= min(_hd.vgain, self.vgain)

            self.pmin_technological = max(self.pmin, self.pmin_heat, self.pmin_tech)

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
        self.pmin_technological = 0
