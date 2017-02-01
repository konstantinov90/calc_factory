"""Class Setting."""
from ..meta_base import MetaBase


class Setting(object, metaclass=MetaBase):
    """class Setting"""
    def __init__(self, ss_row):
        self.code, self.value_type, self.string_value, self.number_value, self.target_date = ss_row
        self._init_on_load()

    def _init_on_load(self):
        """additional initialization"""
        if self.code not in self.lst.keys():
            self.lst[self.code] = self

    def __repr__(self):
        if self.value_type == 'D':
            value = self.target_date
        elif self.value_type == 'N':
            value = self.number_value
        elif self.value_type == 'S':
            value = self.string_value
        return '<Setting %s -> %r>' % (self.code, value)

    def get_settings_data(self):
        """get eq_db_settings view data"""
        return (self.code, self.value_type,
                'null' if self.string_value is None else self.string_value,
                (self.number_value,), (self.target_date,))
