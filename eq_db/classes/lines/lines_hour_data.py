"""Class LineHourData."""

class LineHourData(object):
    """class LineHourData"""
    def __init__(self, ls_row, parent):
        # self.line_id = line_id
        self.line = parent
        *_, self.hour, _, self.r, self.x, self.b, self.g, \
            self.b_from, self.b_to, self.losses = ls_row
        self.state = not ls_row.state

    def __repr__(self):
        return '<LineHourData %i: %s>' % (self.hour, 'ON' if self.state else 'OFF')

    def is_line_on(self):
        """get instance state"""
        return self.state

    def turn_off(self):
        """turn line hour off"""
        self.state = False
        print('%r turned off at hour %i' % (self.line, self.hour))
