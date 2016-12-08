from sqlalchemy import *
from sqlalchemy.orm import relationship
from utils.ORM import Base

from sql_scripts import line_groups_script as lgs


class SectionHourLineData(Base):
    __tablename__ = 'sections_hour_line_data'
    section_hour_id = Column(Integer, ForeignKey('sections_hour_data.id'), primary_key=True)
    node_from_code = Column(Integer, primary_key=True)
    node_to_code = Column(Integer, primary_key=True)
    skip = Column(Boolean)
    div = Column(Numeric)

    lines = relationship('Line', foreign_keys='[Line.node_from_code, Line.node_to_code]', primaryjoin='''or_(
and_(Line.node_from_code == SectionHourLineData.node_from_code, Line.node_to_code == SectionHourLineData.node_to_code),
and_(Line.node_to_code == SectionHourLineData.node_from_code, Line.node_from_code == SectionHourLineData.node_to_code)
)''')

    def __init__(self, lgs_row, id):
        self.section_hour_id = id
        self.node_from_code = lgs_row[lgs['node_from_code']]
        self.node_to_code = lgs_row[lgs['node_to_code']]
        self.skip = True if lgs_row[lgs['state']] else False
        self.div = lgs_row[lgs['div']]

    def __repr__(self):
        return '<SectionHourLineData> %i -> %i: %s' % (self.node_from_code, self.node_to_code, 'ON' if self.state else 'OFF')
