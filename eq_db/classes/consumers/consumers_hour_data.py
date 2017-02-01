"""Class ConsumerHourData."""


class ConsumerHourData(object):
    """class ConsumerHourData"""
    def __init__(self, rc_row):
        self.hour, self.consumer_code, self.type, self.pdem, *_ = rc_row
        # self.type  # 1 - система, 0 - нагрузка

    def __repr__(self):
        return '<ConsumerHourData %i: %i>' % (self.consumer_code, self.hour)
