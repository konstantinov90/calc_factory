"""Class LineHourData."""

class LineHourData(object):
    """class LineHourData"""
    def __init__(self, ls_row, line_id):
        self.line_id = line_id
        *_, self.hour, _, self.r, self.x, self.b, self.g, \
            self.b_from, self.b_to, self.losses = ls_row
        self.state = True if not ls_row.state else False

    def __repr__(self):
        return '<LineHourData %i: %s>' % (self.hour, 'ON' if self.state else 'OFF')

    def is_line_on(self):
        """get instance state"""
        return self.state
