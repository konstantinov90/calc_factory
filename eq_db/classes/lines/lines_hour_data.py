from sqlalchemy import *
from sqlalchemy.orm import relationship

from utils.ORM import Base
from sql_scripts import lines_script as ls

# from .lines import Line


class LineHourData(Base):
    __tablename__ = 'lines_hour_data'
    # id = Column(Integer, primary_key=True)
    line_id = Column(Integer, ForeignKey('lines.id', ondelete='CASCADE'), primary_key=True)
    hour = Column(Integer, primary_key=True)
    r = Column(Numeric)
    x = Column(Numeric)
    b = Column(Numeric)
    g = Column(Numeric)
    b_from = Column(Numeric)
    b_to = Column(Numeric)
    losses = Column(Numeric)
    state = Column(Boolean)

    # line = relationship('Line', primaryjoin='Line.id==LineHourData.line_id', backref='hours')
    line = relationship('Line', back_populates='hour_data')

    def __init__(self, ls_row, line_id):
        self.line_id = line_id
        self.hour = ls_row[ls['hour']]
        self.r = ls_row[ls['r']]
        self.x = ls_row[ls['x']]
        self.b = ls_row[ls['b']]
        self.g = ls_row[ls['g']]
        self.b_from = ls_row[ls['b_from']]
        self.b_to = ls_row[ls['b_to']]
        self.losses = ls_row[ls['losses']]
        self.state = True if not ls_row[ls['state']] else False

    def is_line_on(self):
        return self.state
