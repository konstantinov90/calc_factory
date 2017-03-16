"""Class Node."""
from operator import attrgetter
import constants as C
from ..meta_base import MetaBase
from .nodes_hour_data import NodeHourData


class Node(object, metaclass=MetaBase):
    """class Node"""
    siberia_balance_node = [0]*C.HOURCOUNT
    balance_nodes = [[] for hour in range(C.HOURCOUNT)]

    def __init__(self, ns_row, is_new=False):
        self._hour_data = {}
        self.code, self.area_code, self.nominal_voltage, self.min_voltage, \
            self.max_voltage, self.price_zone, *_ = ns_row
        self.voltage_class = self.get_voltage_class(self.nominal_voltage)
        self.is_new = is_new
        self.area = None
        self.dpg_system = None
        self.dpg_fsk = None
        self.dgus = []
        self.load_node = []
        self.lines = []
        self._init_on_load()

    @property
    def hour_data(self):
        """hour_data property"""
        return sorted(self._hour_data.values(), key=attrgetter('hour'))

    @property
    def p_n(self):
        """sum p_n"""
        return sum(max(_hd.pn, 0) for _hd in self._hour_data.values() if _hd.state)

    def _init_on_load(self):
        """additional initialization"""
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

    # def modify_state(self):
    #     """modify node state"""
    #     for node_hd in self.hour_data:
    #         if not sum(dgu.hour_data[node_hd.hour].pmax for dgu in self.dgus) \
    #            and not node_hd.pn:
    #             node_hd.turn_off()

    def set_hour_balance_node(self, hour, node_code):
        """set balance node at hour"""
        self.hour_data[hour].set_balance_node(node_code)

    def get_hour_balance_node(self, hour):
        """get balance node at hour"""
        return self.hour_data[hour].balance_node_code

    def set_k_distr(self, hour, value):
        """set k_distr for DpgDemandSystem"""
        self.hour_data[hour].set_k_distr(value)

    @staticmethod
    def get_voltage_class(voltage):
        """get voltage class"""
        margin = 0.2
        voltage_classes = [2.8, 4.3, 6.4, 9.6, 14.4, 21.6, 32.4, 48.7, 73., 110.,
                           150., 220., 330., 500., 750., 1150.]
        for v_cl in voltage_classes:
            if abs(voltage / v_cl - 1) < margin:
                return v_cl
        return voltage

    @staticmethod
    def set_siberia_balance_node(hour, node_code):
        """set balance node code for Siberia"""
        Node.siberia_balance_node[hour] = node_code

    @staticmethod
    def get_siberia_balance_node(hour):
        """get balance node code for Siberia"""
        return Node.siberia_balance_node[hour]

    def get_price_zone_code(self):
        """get price zone code by instance.code"""
        return 1 if self.code <= 999999 else 2

    def _check_hour_data(self, ns_row):
        """check data consistency"""
        for datum in ['area_code', 'nominal_voltage', 'price_zone', 'min_voltage', 'max_voltage']:
            self._check_datum(datum, getattr(ns_row, datum))

    def _check_datum(self, attr, datum):
        """check single property consistency"""
        if getattr(self, attr) != datum:
            raise Exception('%s not consistent for node %i' % (attr, self.code))

    def add_to_pdem(self, hour, value):
        """add volume to NodeHourData instance pdem property"""
        self.hour_data[hour].add_to_pdem(value)

    def add_to_retail(self, hour, value):
        """add volume to NodeHourData instance retail property"""
        self.hour_data[hour].add_to_retail(value)

    def add_node_hour_data(self, ns_row):
        """add NodeHourData instance"""
        hour = ns_row.hour
        if hour in self._hour_data.keys():
            raise Exception('tried to insert one chunk of node hour data twice for node %i!'
                            % self.code)
        self._check_hour_data(ns_row)
        _hd = NodeHourData(ns_row, self)
        self._hour_data[hour] = _hd
        # проверка, не балансирующий ли это узел в час hour
        if _hd.is_balance_node() and _hd.is_node_on():
            self.balance_nodes[hour].append(self.code)
        # заполнение псевдо-балансирующего узла Сибири
        if self.code in C.siberia_balance_nodes:
            if not self.get_siberia_balance_node(hour):
                self.set_siberia_balance_node(hour, self.code)
            else:
                cur_index = C.siberia_balance_nodes.index(
                    self.get_siberia_balance_node(hour))
                index = C.siberia_balance_nodes.index(self.code)
                if index < cur_index:
                    self.set_siberia_balance_node(hour, self.code)

    def get_eq_db_nodes_data(self):
        """get eq_db_nodes view data"""
        if not self.eq_db_nodes_data:
            self.prepare_eq_db_nodes_data()
        return self.eq_db_nodes_data

    def get_shunt_data(self):
        """get eq_db_shunts view data"""
        if not self.shunt_data:
            self.prepare_shunts_data()
        return self.shunt_data

    def get_node_hour_type(self, hour):
        """get node type at hour"""
        return self.hour_data[hour].type

    def get_node_hour_load(self, hour):
        """get node load at hour"""
        return self.hour_data[hour].pn

    def get_node_hour_state(self, hour):
        """get node state at hour"""
        return self.hour_data[hour].is_node_on()

    def get_fixed_impex_value(self, hour):
        """get fixed import/export value"""
        impex_value = 0
        if self.area:
            if self.area.impex_data:
                impex_value = self.hour_data[hour].pn - self.hour_data[hour].pg
        return impex_value

    def get_fixed_gen_value(self, hour):
        """get fixed generation value"""
        gen_value = 0
        # for dgu in self.dgus:
        #     if dgu.dpg.is_unpriced_zone:
        #         continue
        #     if not dgu.dpg.is_gaes and not dgu.dpg.is_blocked:
        #         continue
        #     dgu_hd = dgu.hour_data[hour]
        #     if dgu_hd.p:
        #         gen_value += dgu_hd.p
        return gen_value

    def get_fixed_con_value(self, hour):
        """get fixed consumption value"""
        con_value = 0
        _hd = self.hour_data[hour]
        # --------- DPGLOAD ---------
        # for _ln in self.load_node:
        #     dpg = _ln.load.dpg
        #     if not dpg:
        #         continue
        #     if dpg.is_unpriced_zone or not dpg.is_fed_station or dpg.is_system:
        #         continue
        #     p_n = dpg.load.hour_data[_hd.hour].pn
        #     if dpg.load.hour_data[_hd.hour].hour != _hd.hour:
        #         print(self.code, dpg.load, _hd.hour)
        #         raise Exception('FIXEDCON NODES_PV_DATA!')
        #     if dpg.is_disqualified:
        #         p_g = sum(dgu.hour_data[_hd.hour].p for dpg_sup in dpg.supply_dpgs
        #                                             for dgu in dpg_sup.dgus)
        #         volume = min(dpg.disqualified_data.coeff * p_g, p_n)
        #     else:
        #         dpg_hd = dpg.consumer.hour_data[_hd.hour]
        #         if dpg_hd.hour != _hd.hour:
        #             raise Exception('FIXEDCON NODES_PV_DATA!')
        #         if min(0.25 * p_n, p_n - 110) <= dpg_hd.pdem <= max(1.5 * p_n, p_n + 110):
        #             volume = dpg_hd.pdem
        #         else:
        #             volume = p_n
        #     con_value += _ln.hour_data[_hd.hour].node_dose / 100 * volume
        # ---------- DPGSYSTEM --------------
        # for dpg in [self.dpg_system]:
        #     if not dpg:
        #         break
        #     if dpg.is_unpriced_zone and dpg.code not in UNPRICED_AREA:
        #         break
        #     if dpg.is_gp:
        #         break
        #     if dpg.code in UNPRICED_AREA:
        #         con_value += max(_hd.pn, 0)
        #         break
        #     if dpg.is_fed_station:
        #         if not dpg.area.p_n:
        #             break
        #         if dpg.is_disqualified:
        #             if not dpg.disqualified_data.attached_supplies_gen:
        #                 volume = 0
        #             elif not dpg.disqualified_data.fed_station_cons:
        #                 volume = 0
        #             else:
        #                 p_g = sum(dgu.hour_data[_hd.hour].p for dpg_sup in dpg.supply_dpgs
        #                                                     for dgu in dpg_sup.dgus)
        #                 if p_g:
        #                     volume = min(dpg.disqualified_data.coeff * p_g, dpg.area.p_n)
        #                 else:
        #                     volume = dpg.area.p_n
        #         else:
        #             losses = dpg.area.hour_data[_hd.hour].losses
        #             dpg_hd = dpg.consumer.hour_data[_hd.hour]
        #             volume = max(dpg_hd.pdem - losses, 0)
        #         con_value += max(_hd.pn, 0) / dpg.area.p_n * volume
        # -------------- DPGFSK ---------------
        for dpg in [self.dpg_fsk]:
            if not dpg:
                break
            if dpg.is_unpriced_zone or not dpg.area:
                break
            if _hd.pn > 0:
                con_value += _hd.pn

        return con_value

    def get_sw_data(self):
        """get eq_db_nodes_sw view data"""
        if not self.sw_data:
            self.prepare_sw_data()
        return self.sw_data

    def prepare_sw_data(self):
        """prepare eq_db_nodes_sw view data"""
        for _hd in self.hour_data:
            if not _hd.state or _hd.type != 0:
                continue

            relative_voltage = (_hd.fixed_voltage if _hd.fixed_voltage else self.nominal_voltage)\
                                                                             / self.voltage_class

            self.sw_data.append((
                _hd.hour, self.code, self.voltage_class, relative_voltage, 0, -_hd.pn,
                max(_hd.max_q, -10000), min(_hd.min_q, 10000), self.code == C.MAIN_NODE_FOR_DR
            ))

    def get_pv_data(self):
        """get eq_db_nodes_pv view data"""
        if not self.pv_data:
            self.prepare_pv_data()
        return self.pv_data

    def prepare_pv_data(self):
        """prepare eq_db_nodes_pv view data"""
        for _hd in self.hour_data:
            if not _hd.state or _hd.type <= 1:
                continue

            ########### FIXEDIMPEX ##########
            impex_value = -self.get_fixed_impex_value(_hd.hour)
            ########### FIXEDGEN ############
            gen_value = self.get_fixed_gen_value(_hd.hour)
            ########### FIXEDCON ############
            con_value = self.get_fixed_con_value(_hd.hour)

            self.pv_data.append((
                _hd.hour, self.code, self.voltage_class, impex_value + gen_value - con_value,
                _hd.qn, _hd.qg, _hd.type, _hd.fixed_voltage, max(_hd.max_q, -10000),
                min(_hd.min_q, 10000), 1.4 * self.voltage_class, 0.75 * self.voltage_class,
                -min(impex_value, 0) + con_value, max(impex_value, 0) + gen_value
            ))

    def get_pq_data(self):
        """get eq_db_nodes_pq view data"""
        if not self.pq_data:
            self.prepare_pq_data()
        return self.pq_data

    def prepare_pq_data(self):
        """prepare eq_db_nodes_pq view data"""
        for _hd in self.hour_data:
            if not _hd.is_node_on() or _hd.type != 1:
                continue

            ########### FIXEDIMPEX ##########
            impex_value = self.get_fixed_impex_value(_hd.hour)
            ########### FIXEDGEN ############
            gen_value = -self.get_fixed_gen_value(_hd.hour)
            ########### FIXEDCON ############
            con_value = self.get_fixed_con_value(_hd.hour)

            self.pq_data.append((
                _hd.hour, self.code, self.voltage_class, impex_value + gen_value + con_value,
                _hd.qn, _hd.qg, 1.4 * self.voltage_class, 0.75 * self.voltage_class,
                max(impex_value, 0) + con_value, -min(impex_value, 0) - gen_value
            ))

    def prepare_eq_db_nodes_data(self):
        """prepare eq_db_nodes view data"""
        for _hd in self.hour_data:
            if _hd.is_node_on():
                voltage, phase = _hd.get_eq_db_node_hour()
                price_zone = self.price_zone if self.price_zone else 1 if self.code < 1000000 else 2
                if _hd.is_balance_node():  # балансирующий узел
                    price_zone_fixed = price_zone
                elif self.code == self.get_siberia_balance_node(_hd.hour) or \
                        self.area_code in C.fixed_siberia_areas:
                        # self.code in C.fixed_siberia_nodes:
                    price_zone_fixed = 2
                elif self.area_code in C.fixed_europe_areas:
                    price_zone_fixed = 1
                else:
                    price_zone_fixed = -1

                if self.area and self.area.impex_data:
                    in_price_zone = 0
                else:
                    in_price_zone = 1

                self.eq_db_nodes_data.append((
                    _hd.hour, self.code, self.voltage_class, voltage, phase,
                    self.nominal_voltage, self.area_code if self.area_code else 0,
                    price_zone, price_zone_fixed, in_price_zone
                ))


    # def get_fixedcon_data(self):
    #     if not self.fixedcon_data:
    #         self.prepare_fixedcon_data()
    #     return self.fixedcon_data
    #
    # def prepare_fixedcon_data_load(self):
    #     for ln in self.load_node:
    #         dpg = ln.load.dpg
    #         if not dpg:
    #             continue
    #         if dpg.is_unpriced_zone or not dpg.is_fed_station:
    #             continue
    #         for _hd in dpg.consumer.hour_data:
    #             p_n = dpg.load.hour_data[_hd.hour].pn
    #             if dpg.is_disqualified:
    #                 coeff = dpg.disqualified_data.fed_station_cons \
    #                         / dpg.disqualified_data.attached_supplies_gen
    #
    #                 p_g = 0
    #                 for d in dpg.supply_dpgs:
    #                     for dgu in d.dgus:
    #                         p_g += dgu.hour_data[_hd.hour].p
    #                 volume = min(coeff * p_g, p_n)
    #             else:
    #                 if min(0.25 * p_n, p_n - 110) <= _hd.pdem <= max(1.5 * p_n, p_n + 110):
    #                     volume = _hd.pdem
    #                 else:
    #                     volume = p_n
    #             node_type = self.get_node_hour_type(_hd.hour)
    #             if node_type == 0:
    #                 sign = 0
    #             elif node_type != 1:
    #                 sign = -1
    #             else:
    #                 sign = 1
    #             value = ln.hour_data[_hd.hour].node_dose / 100 * volume * sign
    #             if value:
    #                 self.fixedcon_data.append((
    #                     _hd.hour, self.code, dpg.code, value
    #                 ))
    #
    # def prepare_fixedcon_data_system(self):
    #     dpg = self.dpg_system
    #     if not dpg:
    #         return
    #     if dpg.is_unpriced_zone and dpg.code not in UNPRICED_AREA:
    #         return
    #     if dpg.is_gp:
    #         return
    #     if dpg.is_fed_station or dpg.code in UNPRICED_AREA:
    #         for hd in dpg.consumer.hour_data:
    #             if not self.get_node_hour_state(hd.hour):
    #                 continue
    #             p_n = sum([max(node.get_node_hour_load(hd.hour),0)
    #                      for node in dpg.area.nodes if node.get_node_hour_state(hd.hour)])
    #             if not p_n:
    #                 continue
    #             if dpg.is_disqualified:
    #                 if not dpg.disqualified_data.attached_supplies_gen:
    #                     volume = p_n
    #                 if not dpg.disqualified_data.fed_station_cons:
    #                     volume = p_n
    #                 else:
    #                     coeff = dpg.disqualified_data.fed_station_cons \
    #                           / dpg.disqualified_data.attached_supplies_gen
    #                     p_g = 0
    #                     for d in dpg.supply_dpgs:
    #                         for dgu in d.dgus:
    #                             p_g += dgu.hour_data[hd.hour].p
    #                     if p_g:
    #                         volume = min(coeff * p_g, p_n)
    #                     else:
    #                         volume = p_n
    #             else:
    #                 losses = dpg.area.hour_data[hd.hour].losses
    #                 volume = max(hd.pdem - losses, 0)
    #             node_type = self.get_node_hour_type(hd.hour)
    #             if node_type == 0:
    #                 sign = 0
    #             elif node_type != 1:
    #                 sign = -1
    #             else:
    #                 sign = 1
    #             if dpg.code in UNPRICED_AREA:
    #                 value = max(self.get_node_hour_load(hd.hour),0)
    #             else:
    #                 value = max(self.get_node_hour_load(hd.hour),0) / p_n * volume * sign
    #             if value:
    #                 self.fixedcon_data.append((
    #                     hd.hour, self.code, dpg.code, value
    #                 ))
    #
    # def prepare_fixedcon_data_fsk(self):
    #     dpg = self.dpg_fsk
    #     if not dpg:
    #         return
    #     if dpg.is_unpriced_zone or not dpg.area:
    #         return
    #     for hd in self.hour_data:
    #         if hd.pn > 0:
    #             if hd.type == 0:
    #                 sign = 0
    #             elif hd.type != 1:
    #                 sign = -1
    #             else:
    #                 sign = 1
    #             self.fixedcon_data.append((
    #                 hd.hour, self.code, dpg.code, hd.pn * sign
    #             ))
    #
    # def prepare_fixedcon_data(self):
    #     self.prepare_fixedcon_data_load()
    #     self.prepare_fixedcon_data_system()
    #     self.prepare_fixedcon_data_fsk()
    #
    # def get_fixedgen_data(self):
    #     if not self.fixedgen_data:
    #         self.prepare_fixedgen_data()
    #     return self.fixedgen_data
    #
    # def prepare_fixedgen_data(self):
    #     for dgu in self.dgus:
    #         if dgu.dpg.is_unpriced_zone:
    #             continue
    #         if dgu.dpg.is_blocked or dgu.dpg.is_gaes:
    #             for hd in dgu.hour_data:
    #                 if hd.p and self.get_node_hour_state(hd.hour):
    #                     volume = hd.p * (-1 if self.get_node_hour_type(hd.hour) == 1 else 1)
    #                     self.fixedgen_data.append((
    #                     hd.hour, dgu.code, self.code, dgu.dpg.code, volume
    #                     ))
    #
    # def prepare_data(self):
    #     self.prepare_eq_db_nodes_data()
    #     self.prepare_fixedimpex_data()
    #
    # def get_fixedimpex_data(self):
    #     if not self.fixedimpex_data:
    #         self.prepare_fixedimpex_data()
    #     return self.fixedimpex_data

    # def prepare_fixedimpex_data(self):
    #     if not self.area.impex_data:
    #         return
    #     for hd in self.hour_data:
    #         if hd.is_node_on():
    #             if hd.type == 1:
    #                 volume = hd.pn - hd.pg
    #             elif not hd.is_balance_node():
    #                 volume = hd.pg - hd.pn
    #             if volume:
    #                 self.fixedimpex_data.append((
    #                     hd.hour, self.code, volume
    #                 ))

    def prepare_shunts_data(self):
        """prepare eq_db_shunts view data"""
        for _hd in self.hour_data:
            if _hd.state and (_hd.b_shr or _hd.g_shunt):
                self.shunt_data.append((
                    _hd.hour, self.code, self.voltage_class, _hd.g_shunt, -_hd.b_shr
                ))

    def set_area_and_ret(self, area):
        """set instance's area property and return self"""
        self.area = area
        return self

    def add_dgu(self, dgu):
        """add Dgu instance"""
        if dgu in self.dgus:
            raise Exception('tried to attach same dgu %i twice to node %i' % (dgu.code, self.code))
        self.dgus.append(dgu)

    def set_dpg(self, dpg):
        """set DpgDemandSystem or DpgDemandFSK (single in given area)"""
        if not dpg:
            return
        if dpg.is_system:
            self.dpg_system = self.area.dpg
        elif dpg.is_fsk:
            self.dpg_fsk = self.area.dpg
        else:
            raise Exception('area %i has wrong gtp %s' % (self.area.code, self.area.dpg.code))

    def add_load_node(self, load_node):
        """add LoadNodeData instances"""
        self.load_node.append(load_node)

    def add_line(self, line):
        """add Line instances"""
        if line not in self.lines:
            self.lines.append(line)
