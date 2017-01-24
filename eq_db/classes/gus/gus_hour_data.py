"""Class GuHourData."""


class GuHourData(object):
    """class GuHourData"""
    def __init__(self, ns_row):
        self.hour, _, self.gu_code, self.pmin, self.pmax, self.pmin_t, \
        self.pmax_t, _, self.vgain, self.vdrop, _, self.repair = ns_row
        self.state = True if ns_row.state else False
        self.is_sysgen = True if ns_row.is_sysgen else False
        self.changed = False

    def __repr__(self):
        return '<GuHourData %i: %i>' % (self.gu_code, self.hour)
