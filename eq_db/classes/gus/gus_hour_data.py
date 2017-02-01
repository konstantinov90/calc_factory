"""Class GuHourData."""


class GuHourData(object):
    """class GuHourData"""
    def __init__(self, ns_row):
        self.hour, _, self.gu_code, self.pmin, self.pmax, self.pmin_t, \
        self.pmax_t, _, self.vgain, self.vdrop, _, self.repair = ns_row
        self.state = bool(ns_row.state)
        self.is_sysgen = bool(ns_row.is_sysgen)
        self.changed = False

    def __repr__(self):
        return '<GuHourData %i: %i>' % (self.gu_code, self.hour)
