"""Class AreaHourData."""

class AreaHourData(object):
    """class AreaHourData"""
    def __init__(self, ra_row):
        self.hour, self.area_code, self.losses, self.load_losses = ra_row
        self.sum_pn_retail_diff = None
        self.nodes_on = None

    def __repr__(self):
        return '<AreaHourData %i: nodes_on %i>' % (self.hour, self.nodes_on)
