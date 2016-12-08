import itertools
from decimal import Decimal
from sqlalchemy import *
from sqlalchemy.orm import relationship, reconstructor
# from sqlalchemy.ext.declarative import declarative_base

from utils.ORM import Base, MetaBase
from .nodes_hour_data import NodeHourData

from sql_scripts import nodes_script as ns, impex_area_script as ias
import settings

UNPRICED_AREA = ('PCHITAZN', 'PAMUREZN')
HOURCOUNT = 24


class Node(Base, metaclass=MetaBase):
    __tablename__ = 'nodes'
    code = Column(Integer, primary_key=True)
    area_code = Column(Integer, ForeignKey('areas.code'), nullable=True)
    nominal_voltage = Column(Numeric)
    price_zone = Column(Integer)
    min_voltage = Column(Numeric)
    max_voltage = Column(Numeric)
    voltage_class = Column(Numeric)

    hour_data = relationship('NodeHourData', order_by='NodeHourData.hour', lazy='subquery')
    lines = relationship('Line', primaryjoin='or_(Node.code == Line.node_from_code, Node.code == Line.node_to_code)')
    area = relationship('Area', primaryjoin='Node.area_code == Area.code')
    dgus = relationship('Dgu', back_populates='node')
    dpg_system = relationship('DpgDemandSystem', uselist=False, primaryjoin='DpgDemandSystem.area_code == Node.area_code', foreign_keys='DpgDemandSystem.area_code')
    dpg_fsk = relationship('DpgDemandFSK', uselist=False, primaryjoin='DpgDemandFSK.area_code == Node.area_code', foreign_keys='DpgDemandFSK.area_code')
    load_node = relationship('LoadNodeData', primaryjoin='LoadNodeData.node_code == Node.code', foreign_keys='LoadNodeData.node_code')

    siberia_balance_node = [0]*HOURCOUNT
    balance_nodes = [[] for hour in range(HOURCOUNT)]

    def __init__(self, ns_row):
        self.hours = {}  # [0] * HOURCOUNT
        self.code = ns_row[ns['node_code']]
        self.area_code = ns_row[ns['area_code']]
        self.nominal_voltage = ns_row[ns['nominal_voltage']]
        self.price_zone = ns_row[ns['price_zone']]
        self.min_voltage = ns_row[ns['min_voltage']]
        self.max_voltage = ns_row[ns['max_voltage']]
        self.voltage_class = self.get_voltage_class(self.nominal_voltage)
        self._init_on_load()

    @reconstructor
    def _init_on_load(self):
        if self.code not in self.lst.keys():
            self.lst[self.code] = self
        self.eq_db_nodes_data = []
        self.fixedimpex_data = []
        self.shunt_data = []
        self.fixedgen_data = []
        self.fixedcon_data = []
        self.pq_data = []
        self.pv_data = []
        self.sw_data = []

    def __repr__(self):
        return '<Node %i>' % self.code

    def serialize(self, session):
        session.add(self)
        session.add_all(self.hours.values())
        session.flush()

    def turn_on_hour(self, hour):
        self.hour_data[hour].turn_on()

    def set_hour_balance_node(self, hour, node_code):
        self.hour_data[hour].set_balance_node(node_code)

    def get_hour_balance_node(self, hour):
        return self.hour_data[hour].balance_node_code

    def set_k_distr(self, hour, value):
        self.hour_data[hour].set_k_distr(value)

    @staticmethod
    def get_voltage_class(v):
        margin = 0.2
        voltage_classes = [2.8, 4.3, 6.4, 9.6, 14.4, 21.6, 32.4, 48.7, 73., 110.,
                           150., 220., 330., 500., 750., 1150.]
        for cl in voltage_classes:
            if abs(v / cl - 1) < margin:
                return cl
        return v

    @staticmethod
    def set_siberia_balance_node(hour, node_code):
        Node.siberia_balance_node[hour] = node_code

    @staticmethod
    def set_impex_area(impex_area):
        Node.impex_area = [i[ias['area']] for i in impex_area if i[ias['area']]]

    @staticmethod
    def get_siberia_balance_node(hour):
        return Node.siberia_balance_node[hour]

    def get_price_zone_code(self):
        return 1 if self.code <= 999999 else 2

    def __str__(self):
        return'%i hours: %i' % (self.code, len(self.hour_data))

    def _check_hour_data(self, ns_row):
        for d in ['area_code', 'nominal_voltage', 'price_zone', 'min_voltage', 'max_voltage']:
            self._check_datum(d, ns_row[ns[d]])

    def add_to_pdem(self, hour, value):
        self.hour_data[hour].add_to_pdem(value)

    def add_to_retail(self, hour, value):
        self.hour_data[hour].add_to_retail(value)

    def add_node_hour_data(self, ns_row):
        hour = ns_row[ns['hour']]
        if hour in self.hours.keys():
            raise Exception('tried to insert one chunk of node hour data twice for node %i!' % self.code)
        self._check_hour_data(ns_row)
        hd = NodeHourData(ns_row)
        self.hours[hour] = hd
        # проверка, не балансирующий ли это узел в час hour
        if hd.is_balance_node() and hd.is_node_on():
            self.balance_nodes[hour].append(self.code)
        # заполнение псевдо-балансирующего узла Сибири
        if self.code in settings.siberia_balance_nodes:
            if not self.get_siberia_balance_node(hour):
                self.set_siberia_balance_node(hour, self.code)
            else:
                cur_index = settings.siberia_balance_nodes.index(self.get_siberia_balance_node(hour))
                index = settings.siberia_balance_nodes.index(self.code)
                if index < cur_index:
                    self.set_siberia_balance_node(hour, self.code)

    def _check_datum(self, attr, datum):
        if getattr(self, attr) != datum:
            raise Exception('%s not consistent for node %i' % (attr, self.code))

    def get_eq_db_nodes_data(self):
        if not self.eq_db_nodes_data:
            self.prepare_eq_db_nodes_data()
        return self.eq_db_nodes_data

    def get_fixedimpex_data(self):
        if not self.fixedimpex_data:
            self.prepare_fixedimpex_data()
        return self.fixedimpex_data

    def get_shunt_data(self):
        if not self.shunt_data:
            self.prepare_shunts_data()
        return self.shunt_data

    def get_node_hour_type(self, hour):
        return self.hour_data[hour].type

    def get_node_hour_load(self, hour):
        return self.hour_data[hour].pn

    def get_node_hour_state(self, hour):
        return self.hour_data[hour].is_node_on()

    def get_fixed_impex_value(self, hour):
        impex_value = 0
        if self.area:
            if self.area.impex_data:
                impex_value = self.hour_data[hour].pn - self.hour_data[hour].pg
        return impex_value

    def get_fixed_gen_value(self, hour):
        gen_value = 0
        for dgu in self.dgus:
            if dgu.dpg.is_unpriced_zone:
                continue
            if not dgu.dpg.is_blocked and not dgu.dpg.is_gaes:
                continue
            dgu_hd = dgu.hour_data[hour]
            if dgu_hd.p:
                gen_value += dgu_hd.p
        return gen_value

    def get_fixed_con_value(self, hour):
        con_value = 0
        hd = self.hour_data[hour]
        # --------- DPGLOAD ---------
        for ln in self.load_node:
            dpg = ln.load.dpg
            if not dpg:
                continue
            if dpg.is_unpriced_zone or not dpg.is_fed_station:
                continue
            p_n = dpg.load.hour_data[hd.hour].pn
            if dpg.load.hour_data[hd.hour].hour != hd.hour:
                print(self.code, dpg.load, hd.hour )
                raise Exception('FIXEDCON NODES_PV_DATA!')
            if dpg.is_disqualified:
                ddd = dpg.disqualified_data
                coeff = ddd.fed_station_cons / ddd.attached_supplies_gen
                p_g = 0
                for d in dpg.supply_dpgs:
                    for dgu in d.dgus:
                        p_g += dgu.hour_data[hd.hour].p
                volume = min(coeff * p_g, p_n)
            else:
                dpg_hd = dpg.consumer.hour_data[hd.hour]
                if dpg_hd.hour != hd.hour:
                    raise Exception('FIXEDCON NODES_PV_DATA!')
                if min(Decimal(0.25) * p_n, p_n - 110) <= dpg_hd.pdem <= max(Decimal(1.5) * p_n, p_n + 110):
                    volume = dpg_hd.pdem
                else:
                    volume = p_n
            con_value += ln.hour_data[hd.hour].node_dose / 100 * volume
        # ---------- DPGSYSTEM --------------
        for dpg in [self.dpg_system]:
            if not dpg:
                break
            if dpg.is_unpriced_zone and dpg.code not in UNPRICED_AREA:
                break
            if dpg.is_gp:
                break
            if dpg.code in UNPRICED_AREA:
                con_value += max(hd.pn, 0)
                break
            if dpg.is_fed_station:
                p_n = sum([max(node.get_node_hour_load(hd.hour),0) for node in dpg.area.nodes if node.get_node_hour_state(hd.hour)])
                if not p_n:
                    break
                if dpg.is_disqualified:
                    if not dpg.disqualified_data.attached_supplies_gen:
                        volume = 0
                    elif not dpg.disqualified_data.fed_station_cons:
                        volume = 0
                    else:
                        coeff = dpg.disqualified_data.fed_station_cons / dpg.disqualified_data.attached_supplies_gen
                        p_g = 0
                        for d in dpg.supply_dpgs:
                            for dgu in d.dgus:
                                p_g += dgu.hour_data[hd.hour].p
                        if p_g:
                            volume = min(coeff * p_g, p_n)
                        else:
                            volume = p_n
                else:
                    losses = dpg.area.hour_data[hd.hour].losses
                    dpg_hd = dpg.consumer.hour_data[hd.hour]
                    volume = max(dpg_hd.pdem - losses, 0)
                con_value += max(hd.pn, 0) / p_n * volume
        # -------------- DPGFSK ---------------
        for dpg in [self.dpg_fsk]:
            if not dpg:
                break
            if dpg.is_unpriced_zone or not dpg.area:
                break
            if hd.pn > 0:
                con_value += hd.pn

        return con_value

    def get_sw_data(self):
        if not self.sw_data:
            self.prepare_sw_data()
        return self.sw_data

    def prepare_sw_data(self):
        for hd in self.hour_data:
            if not hd.state or hd.type != 0:
                continue

            relative_voltage = (hd.fixed_voltage if hd.fixed_voltage else self.nominal_voltage)\
                                                                             / self.voltage_class

            self.sw_data.append((
                hd.hour, self.code, self.voltage_class, relative_voltage, 0, -hd.pn,
                max(hd.max_q, -10000), min(hd.min_q, 10000)
            ))

    def get_pv_data(self):
        if not self.pv_data:
            self.prepare_pv_data()
        return self.pv_data

    def prepare_pv_data(self):
        for hd in self.hour_data:
            if not hd.state or hd.type <= 1:
                continue

            ########### FIXEDIMPEX ##########
            impex_value = -self.get_fixed_impex_value(hd.hour)
            ########### FIXEDGEN ############
            gen_value = self.get_fixed_gen_value(hd.hour)
            ########### FIXEDCON ############
            con_value = self.get_fixed_con_value(hd.hour)

            self.pv_data.append((
                hd.hour, self.code, self.voltage_class, impex_value + gen_value - con_value,
                hd.qn, hd.qg, hd.type, hd.fixed_voltage, max(hd.max_q, -10000), min(hd.min_q, 10000),
                Decimal(1.4) * self.voltage_class, Decimal(0.75) * self.voltage_class,
                -min(impex_value, 0) + con_value, max(impex_value, 0) + gen_value
            ))

    def get_pq_data(self):
        if not self.pq_data:
            self.prepare_pq_data()
        return self.pq_data

    def prepare_pq_data(self):
        for hd in self.hour_data:
            if not hd.is_node_on() or hd.type != 1:
                continue

            ########### FIXEDIMPEX ##########
            impex_value = self.get_fixed_impex_value(hd.hour)
            ########### FIXEDGEN ############
            gen_value = -self.get_fixed_gen_value(hd.hour)
            ########### FIXEDCON ############
            con_value = self.get_fixed_con_value(hd.hour)

            self.pq_data.append((
                hd.hour, self.code, self.voltage_class, impex_value + gen_value + con_value,
                hd.qn, hd.qg, Decimal(1.4) * self.voltage_class, Decimal(0.75) * self.voltage_class,
                max(impex_value, 0) + con_value, -min(impex_value, 0) - gen_value
            ))


    def get_fixedcon_data(self):
        if not self.fixedcon_data:
            self.prepare_fixedcon_data()
        return self.fixedcon_data

    def prepare_fixedcon_data_load(self):
        for ln in self.load_node:
            dpg = ln.load.dpg
            if not dpg:
                continue
            if dpg.is_unpriced_zone or not dpg.is_fed_station:
                continue
            for hd in dpg.consumer.hour_data:
                p_n = dpg.load.hour_data[hd.hour].pn
                if dpg.is_disqualified:
                    coeff = dpg.disqualified_data.fed_station_cons / dpg.disqualified_data.attached_supplies_gen

                    p_g = 0
                    for d in dpg.supply_dpgs:
                        for dgu in d.dgus:
                            p_g += dgu.hour_data[hd.hour].p
                    volume = min(coeff * p_g, p_n)
                else:
                    if min(Decimal(0.25) * p_n, p_n - 110) <= hd.pdem <= max(Decimal(1.5) * p_n, p_n + 110):
                        volume = hd.pdem
                    else:
                        volume = p_n
                node_type = self.get_node_hour_type(hd.hour)
                if node_type == 0:
                    sign = 0
                elif node_type != 1:
                    sign = -1
                else:
                    sign = 1
                value = ln.hour_data[hd.hour].node_dose / 100 * volume * sign
                if value:
                    self.fixedcon_data.append((
                        hd.hour, self.code, dpg.code, value
                    ))

    def prepare_fixedcon_data_system(self):
        dpg = self.dpg_system
        if not dpg:
            return
        if dpg.is_unpriced_zone and dpg.code not in UNPRICED_AREA:
            return
        if dpg.is_gp:
            return
        if dpg.is_fed_station or dpg.code in UNPRICED_AREA:
            for hd in dpg.consumer.hour_data:
                if not self.get_node_hour_state(hd.hour):
                    continue
                p_n = sum([max(node.get_node_hour_load(hd.hour),0) for node in dpg.area.nodes if node.get_node_hour_state(hd.hour)])
                if not p_n:
                    continue
                if dpg.is_disqualified:
                    if not dpg.disqualified_data.attached_supplies_gen:
                        volume = p_n
                    if not dpg.disqualified_data.fed_station_cons:
                        volume = p_n
                    else:
                        coeff = dpg.disqualified_data.fed_station_cons / dpg.disqualified_data.attached_supplies_gen
                        p_g = 0
                        for d in dpg.supply_dpgs:
                            for dgu in d.dgus:
                                p_g += dgu.hour_data[hd.hour].p
                        if p_g:
                            volume = min(coeff * p_g, p_n)
                        else:
                            volume = p_n
                else:
                    losses = dpg.area.hour_data[hd.hour].losses
                    volume = max(hd.pdem - losses, 0)
                node_type = self.get_node_hour_type(hd.hour)
                if node_type == 0:
                    sign = 0
                elif node_type != 1:
                    sign = -1
                else:
                    sign = 1
                if dpg.code in UNPRICED_AREA:
                    value = max(self.get_node_hour_load(hd.hour),0)
                else:
                    value = max(self.get_node_hour_load(hd.hour),0) / p_n * volume * sign
                if value:
                    self.fixedcon_data.append((
                        hd.hour, self.code, dpg.code, value
                    ))

    def prepare_fixedcon_data_fsk(self):
        dpg = self.dpg_fsk
        if not dpg:
            return
        if dpg.is_unpriced_zone or not dpg.area:
            return
        for hd in self.hour_data:
            if hd.pn > 0:
                if hd.type == 0:
                    sign = 0
                elif hd.type != 1:
                    sign = -1
                else:
                    sign = 1
                self.fixedcon_data.append((
                    hd.hour, self.code, dpg.code, hd.pn * sign
                ))

    def prepare_fixedcon_data(self):
        self.prepare_fixedcon_data_load()
        self.prepare_fixedcon_data_system()
        self.prepare_fixedcon_data_fsk()

    def get_fixedgen_data(self):
        if not self.fixedgen_data:
            self.prepare_fixedgen_data()
        return self.fixedgen_data

    def prepare_fixedgen_data(self):
        for dgu in self.dgus:
            if dgu.dpg.is_unpriced_zone:
                continue
            if dgu.dpg.is_blocked or dgu.dpg.is_gaes:
                for hd in dgu.hour_data:
                    if hd.p and self.get_node_hour_state(hd.hour):
                        volume = hd.p * (-1 if self.get_node_hour_type(hd.hour) == 1 else 1)
                        self.fixedgen_data.append((
                        hd.hour, dgu.code, self.code, dgu.dpg.code, volume
                        ))

    def prepare_data(self):
        self.prepare_eq_db_nodes_data()
        self.prepare_fixedimpex_data()

    def prepare_eq_db_nodes_data(self):
        for hd in self.hour_data:
            if hd.is_node_on():
                voltage, phase = hd.get_eq_db_node_hour()
                price_zone = self.price_zone if self.price_zone else 1 if self.code < 1000000 else 2
                if hd.is_balance_node():  # балансирующий узел
                    price_zone_fixed = price_zone
                elif self.code == self.get_siberia_balance_node(hd.hour) or \
                        self.area_code in settings.fixed_siberia_areas:
                        # self.code in settings.fixed_siberia_nodes:
                    price_zone_fixed = 2
                elif self.area_code in settings.fixed_europe_areas:
                    price_zone_fixed = 1
                else:
                    price_zone_fixed = -1
                self.eq_db_nodes_data.append((
                    hd.hour, self.code, self.voltage_class, voltage, phase,
                    self.nominal_voltage, self.area_code if self.area_code else 0, price_zone ##, price_zone_fixed
                ))

    def prepare_fixedimpex_data(self):
        if not self.area.impex_data:
            return
        for hd in self.hour_data:
            if hd.is_node_on():
                if hd.type == 1:
                    volume = hd.pn - hd.pg
                elif not hd.is_balance_node():
                    volume = hd.pg - hd.pn
                if volume:
                    self.fixedimpex_data.append((
                        hd.hour, self.code, volume
                    ))

    def prepare_shunts_data(self):
        for hd in self.hour_data:
            if hd.state and (hd.b_shr or hd.g_shunt):
                self.shunt_data.append((
                    hd.hour, self.code, self.voltage_class, hd.g_shunt, -hd.b_shr
                ))

# Node.hours = relationship('NodeHourData', back_populates='node')
# NodeHourData.node = relationship('Node', back_populates='hours')
