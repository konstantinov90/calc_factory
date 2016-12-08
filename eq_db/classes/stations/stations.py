from sqlalchemy import *
from sqlalchemy.orm import relationship, reconstructor

from utils.ORM import Base, MetaBase
from sql_scripts import stations_script as ss


class Station(Base, metaclass=MetaBase):
    # lst = {}
    __tablename__ = 'stations'
    id = Column(Integer, primary_key=True)
    code = Column(String(16), unique=True)
    type = Column(Integer)
    category = Column(Integer)

    dpgs = relationship('DpgSupply', back_populates='station')

    __table_args__ = (
        CheckConstraint('category in (1, 8)', name='category_check'),
        CheckConstraint('type between 1 and 5', name='type_check')
    )

    def __init__(self, ss_row):
        self.id = ss_row[ss['id']]
        self.code = ss_row[ss['code']]
        self.type = ss_row[ss['type']]
        self.category = ss_row[ss['category']]

    @reconstructor
    def _init_on_load(self):
        if self.id not in self.lst.keys():
            self.lst[self.id] = self
