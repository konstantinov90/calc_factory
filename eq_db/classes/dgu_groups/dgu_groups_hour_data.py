"""Class DguGroupHourData."""


class DguGroupHourData(object):
    """class DguGroupHourData"""
    def __init__(self, prs_row):
        self.hour, self.dgu_group_code, _, self.p_min, self.p_max = prs_row
        self.state = True if prs_row.state else False
