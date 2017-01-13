"""Class DpgDisqualified."""


class DpgDisqualified(object):
    """class DpgDisqualified"""
    def __init__(self, dds_row):
        self.dpg_id, _, self.fed_station_cons, self.attached_supplies_gen = dds_row
        self.coeff = self.fed_station_cons / self.attached_supplies_gen
                    #  if self.attached_supplies_gen else None
        if not self.coeff:
            raise Exception('Failed to evaluate disqualified Dpg coeff %r' % dds_row)
