import itertools
from sqlalchemy import *
from sqlalchemy.orm import relationship
from utils.ORM import Base

from sql_scripts import sections_script as ss
from sql_scripts import line_groups_script as lgs

from .sections_hour_line_data import SectionHourLineData


class SectionHourData(Base):
    __tablename__ = 'sections_hour_data'
    id = Column(Integer, unique=True)
    section_code = Column(Integer, ForeignKey('sections.code'), primary_key=True)
    hour = Column(Integer, primary_key=True)
    p_max = Column(Numeric)
    p_min = Column(Numeric)
    state = Column(Boolean)

    line_data = relationship('SectionHourLineData')
    max_price = relationship('BidMaxPrice', uselist=False, primaryjoin='BidMaxPrice.hour == SectionHourData.hour', foreign_keys='BidMaxPrice.hour')

    seq = itertools.count()
    def __init__(self, ss_row):
        self.id = next(self.seq)
        self.section_code = ss_row[ss['code']]
        self.hour = ss_row[ss['hour']]
        self.p_max = ss_row[ss['p_max']]
        self.p_min = ss_row[ss['p_min']]
        self.state = True if ss_row[ss['state']] else False
        self.lines = []

    def add_section_line(self, lgs_row):
        self.lines.append(SectionHourLineData(lgs_row, self.id))

    def __repr__(self):
        return '<SectionHourData> %i: %s' % (self.section_code, 'ON' if self.state else 'OFF')
