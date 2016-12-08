import itertools, math
from sqlalchemy import *
from sqlalchemy.orm import relationship, reconstructor

from utils.ORM import Base, MetaBase
from sql_scripts import lines_script as ls
from .lines_hour_data import LineHourData
from ..nodes import Node

HOURCOUNT = 24


class Line(Base, metaclass=MetaBase):
    __tablename__ = 'lines'
    id = Column(BigInteger, unique=True)
    node_from_code = Column(Integer, ForeignKey('nodes.code'), primary_key=True)
    node_to_code = Column(Integer, ForeignKey('nodes.code'), primary_key=True)
    parallel_num = Column(Integer, primary_key=True)
    kt_re = Column(Numeric)
    kt_im = Column(Numeric)
    div = Column(Numeric)
    type = Column(Integer)
    area = Column(Integer)
    kt_re = Column(Numeric)
    kt_re = Column(Numeric)

    hour_data = relationship('LineHourData', back_populates='line', order_by='LineHourData.hour') #, lazy='subquery')
    node_from = relationship('Node', primaryjoin='Node.code == Line.node_from_code')
    node_to = relationship('Node', primaryjoin='Node.code == Line.node_to_code')

    seq = itertools.count()

    def __init__(self, ls_row):
        self.id = next(self.seq)
        self.node_from_code = ls_row[ls['node_from']]
        self.node_to_code = ls_row[ls['node_to']]
        self.parallel_num = ls_row[ls['n_par']]
        self.kt_re = ls_row[ls['kt_re']]
        self.kt_im = ls_row[ls['kt_im']]
        self.div = ls_row[ls['div']]
        self.type = ls_row[ls['type']]
        self.area = ls_row[ls['area']]
        self.hours = {}  # [0] * HOURCOUNT
        self._init_on_load()

    @reconstructor
    def _init_on_load(self):
        if not self.id in self.lst.keys():
            self.lst[self.id] = self
        # self.node_from = None
        # self.node_to = None
        self.eq_db_lines_data = []
        self.group_line_div = {}
        self.group_line_flipped = {}

    def __repr__(self):
        return '<Line> %i -> %i npar = %i' % (self.node_from_code, self.node_to_code, self.parallel_num)

    def serialize(self, session):
        session.add(self)
        session.add_all(self.hours.values())
        session.flush()

    def attach_nodes(self, nodes_list):
        self.node_from = nodes_list[self.node_from_code]
        self.node_to = nodes_list[self.node_to_code]

    def set_group_line_div(self, section_code, div):
        self.group_line_div[section_code] = div

    def set_group_flipped(self, section_code, flipped):
        self.group_line_flipped[section_code] = flipped

    def get_line_hour_state(self, hour):
        return self.hour_data[hour].is_line_on()

    def add_line_hour_data(self, ls_row):
        self._check_hour_data(ls_row)
        hour = ls_row[ls['hour']]
        self.hours[hour] = LineHourData(ls_row, self.id)

    def _check_hour_data(self, ls_row):
        for d in ['kt_re', 'kt_im', 'div', 'type', 'area']:
            self._check_datum(d, ls_row[ls[d]])

    def _check_datum(self, attr, datum):
        if getattr(self, attr) != datum:
            raise Exception('%s not consistent for line %i - %i n_par = %i'
                            % (attr, self.node_code_from, self.node_code_to, self.parallel_num))

    def get_eq_db_lines_data(self):
        if not self.eq_db_lines_data:
            self.prepare_lines_data()
        return self.eq_db_lines_data

    def prepare_lines_data(self):
        for hd in self.hour_data:
            try:
                if hd.state and self.node_from.get_node_hour_state(hd.hour) and self.node_to.get_node_hour_state(hd.hour):
                    if not self.type:
                        node_start = self.node_from_code
                        node_finish = self.node_to_code
                        base_coeff = 0
                        k_pu = 0
                    else:
                        node_start = self.node_to_code
                        node_finish = self.node_from_code
                        base_coeff = self.node_to.voltage_class / self.node_from.voltage_class
                        k_pu = math.sqrt(math.pow(self.kt_re, 2) + math.pow(self.kt_im, 2))
                    lag = math.atan(self.kt_im / self.kt_re) if self.kt_re else 0

                    self.eq_db_lines_data.append((
                        hd.hour, node_start, node_finish, self.parallel_num, self.type,
                        max(self.node_from.voltage_class, self.node_to.voltage_class), base_coeff,
                        hd.r, hd.x, hd.g, -hd.b, k_pu, lag, -hd.b_from, -hd.b_to
                    ))
            except Exception:
                print('ERROR! line %i-%i has no node(s)' % (self.node_from_code, self.node_to_code))
