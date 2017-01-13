"""Class DguLastHour."""


class DguLastHour(object):
    """class DguLastHour"""
    def __init__(self, glhs_row):
        self.dgu_code, self.volume = glhs_row

    def __repr__(self):
        return '<DguLastHour %i - %f>' % (self.dgu_code, self.volume)

    def get_data(self):
        """get Dgu last hour data"""
        return (self.dgu_code, self.volume)
