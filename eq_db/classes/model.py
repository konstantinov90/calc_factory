HOURCOUNT = 24


class ModelObjectHour(object):
    def __init__(self):
        pass


class ModelObject(object):
    def __init__(self):
        self.singleton_fields = []
        self.hour_fields = []
        self.hour_data = [0]*HOURCOUNT

    def _check_hour_data(self, new_row, row_index):
        for d in self.hour_data:
            self._check_datum(d, new_row[row_index[d]])

    def _check_datum(self, attr, datum):
        if getattr(self, attr) != datum:
            raise Exception('%s not consistent for %s %i' %
                            (attr, self.__class__.__name__, self.get_name()))

    def get_name(self):
        return ''
