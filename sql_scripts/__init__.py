import os


class Script(object):
    def __init__(self, filename):
        self.filename = filename
        self.query = ''
        self.index = {}
        self._read_file()

    def __getitem__(self, item):
        return self.index[item]

    def _read_file(self):
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), self.filename)) as f:
            self.query = f.read()

    def get_query(self):
        return self.query


class GeneratorsScript(Script):
    def __init__(self):
        super().__init__('generators.sql')
        self.index = {
            # 'station_code': 0,
            # 'station_type': 1,
            'dpg_code': 0,
            'gtp_id': 1,
            # 'rge_code': 4,
            'price_zone_code': 2,
            # 'is_pintsch_gas': 5,
            'is_gaes': 3,
            'is_blocked': 4,
            'is_unpriced_zone': 5,
            'fed_station_id': 6,
            'station_id': 7
        }


class GeneratorsScript_V(Script):
    def __init__(self):
        super().__init__('generators_v.sql')
        self.index = {
            # 'station_code': 0,
            # 'station_type': 1,
            'dpg_code': 0,
            'gtp_id': 1,
            # 'rge_code': 4,
            'price_zone_code': 2,
            # 'is_pintsch_gas': 5,
            'is_gaes': 3,
            'is_blocked': 4,
            'is_unpriced_zone': 5,
            'fed_station_id': 6,
            'station_id': 7
        }


class BidsScript(Script):
    def __init__(self):
        super().__init__('bids.sql')
        self.index = {
            'dpg_code': 0,
            'dpg_id': 1,
            'bid_direction': 2,
            'hour': 3,
            'interval_number': 4,
            'price': 5,
            'volume': 6
        }


class BidInitsScript(Script):
    def __init__(self):
        super().__init__('bid_init.sql')
        self.index = {
            'dpg_code': 0,
            'dpg_id': 1,
            'bid_id': 2
        }


class BidInitsScript_V(Script):
    def __init__(self):
        super().__init__('bid_init_v.sql')
        self.index = {
            'dpg_code': 0,
            'dpg_id': 1,
            'bid_id': 2
        }


class BidHoursScript(Script):
    def __init__(self):
        super().__init__('bid_hour.sql')
        self.index = {
            'bid_id': 0,
            'bid_hour_id': 1,
            'hour': 2,
            'dpg_id': 3
        }


class BidHoursScript_V(Script):
    def __init__(self):
        super().__init__('bid_hour_v.sql')
        self.index = {
            'bid_id': 0,
            'bid_hour_id': 1,
            'hour': 2,
            'dpg_id': 3
        }


class BidPairsScript(Script):
    def __init__(self):
        super().__init__('bid_pair.sql')
        self.index = {
            'bid_hour_id': 0,
            'interval_number': 1,
            'price': 2,
            'volume': 3,
            'bid_direction': 4,
            'dpg_id': 5,
            'volume_init': 6
        }


class BidPairsScript_V(Script):
    def __init__(self):
        super().__init__('bid_pair_v.sql')
        self.index = {
            'bid_hour_id': 0,
            'interval_number': 1,
            'price': 2,
            'volume': 3,
            'bid_direction': 4,
            'dpg_id': 5,
            'volume_init': 6
        }


# class KgRgeScript(Script):
#     def __init__(self):
#         super().__init__('kg_rge.sql')
#         self.index = {
#             'hour': 0,
#             'dpg_id': 1,
#             'rge_code': 2,
#             'kg': 3,
#             'kg_reg': 4,
#             'node': 5,
#             'pmin_dpg': 6,
#             'pmin': 7,
#             'pminagg': 8,
#             'pmax': 9,
#             'dpminso': 10,
#             'p': 11,
#             'pminagg_dpg': 12,
#             'pmax_dpg': 13,
#             'kg_min': 14
#         }


class RastrGenScript(Script):
    def __init__(self):
        super().__init__('rastr_gen.sql')
        self.index = {
            'hour': 0,
            'rge_code': 1,
            'pmin': 2,
            'pmax': 3,
            'pmin_agg': 4,
            'pmax_agg': 5,
            'pmin_tech': 6,
            'pmax_tech': 7,
            'pmin_heat': 8,
            'pmax_heat': 9,
            'pmin_so': 10,
            'pmax_so': 11,
            'p': 12,
            'wmax': 13,
            'wmin': 14,
            'vgain': 15,
            'vdrop': 16,
            # 'dpg_id': 10,
            'node_code': 17
        }


class RastrGenScript_V(Script):
    def __init__(self):
        super().__init__('rastr_gen_v.sql')
        self.index = {
            'hour': 0,
            'rge_code': 1,
            'pmin': 2,
            'pmax': 3,
            'pmin_agg': 4,
            'pmax_agg': 5,
            'pmin_tech': 6,
            'pmax_tech': 7,
            'pmin_heat': 8,
            'pmax_heat': 9,
            'pmin_so': 10,
            'pmax_so': 11,
            'p': 12,
            'wmax': 13,
            'wmin': 14,
            'vgain': 15,
            'vdrop': 16,
            # 'dpg_id': 10,
            'node_code': 17
        }


class WsumgenScript(Script):
    def __init__(self):
        super().__init__('wsumgen.sql')
        self.index = {
            'rge_code': 0,
            'price': 1,
            'integral_id': 2,
            'hour_start': 3,
            'hour_end': 4,
            'min_volume': 5,
            'volume': 6
        }


class ConsumersScript(Script):
    def __init__(self):
        super().__init__('consumers.sql')
        self.index = {
            'dpg_code': 0,
            'consumer_code': 1,
            'is_system': 2,
            'is_gp': 3,
            'dpg_id': 4,
            'is_fed_station': 5,
            'is_disqualified': 6,
            'is_unpriced_zone': 7,
            'is_fsk': 8,
            'area': 9,
            'min_forecast': 10,
            'max_forecast': 11
        }


# class KcNodeScript(Script):
#     def __init__(self):
#         super().__init__('kc_node.sql')
#         self.index = {
#             'hour': 0,
#             'node_code': 1,
#             'kc': 2,
#             'kc_by_part': 3,
#             'kc_total': 4,
#             'volume_node_dpg_base': 5,
#             'volume_node_dpg': 6,
#             'dpg_id': 7
#         }


class MaxBidPriceScript(Script):
    def __init__(self):
        super().__init__('max_bid_price.sql')
        self.index = {
            'hour': 0,
            'max_bid_price': 1
        }


class NodesScript(Script):
    def __init__(self):
        super().__init__('nodes.sql')
        self.index = {
            'hour': 0,
            'node_code': 1,
            'area': 2,
            'state': 3,
            'type': 4,
            'nominal_voltage': 5,
            'pn': 6,
            'max_q': 7,
            'min_q': 8,
            'voltage': 9,
            'phase': 10,
            'fixed_voltage': 11,
            'g_shunt': 12,
            'b_shunt': 13,
            'min_voltage': 14,
            'max_voltage': 15,
            'qn': 16,
            'qg': 17,
            'pg': 18,
            'q_shunt': 19,
            'price_zone': 20,
            'p_shunt': 21,
            'b_shr': 22
        }


class NodesScript_V(Script):
    def __init__(self):
        super().__init__('nodes_v.sql')
        self.index = {
            'hour': 0,
            'node_code': 1,
            'area': 2,
            'state': 3,
            'type': 4,
            'nominal_voltage': 5,
            'pn': 6,
            'max_q': 7,
            'min_q': 8,
            'voltage': 9,
            'phase': 10,
            'fixed_voltage': 11,
            'g_shunt': 12,
            'b_shunt': 13,
            'min_voltage': 14,
            'max_voltage': 15,
            'qn': 16,
            'qg': 17,
            'pg': 18,
            'q_shunt': 19,
            'price_zone': 20,
            'p_shunt': 21,
            'b_shr': 22
        }


class ImpexAreaScript(Script):
    def __init__(self):
        super().__init__('impex_na.sql')
        self.index = {
            'section_number': 0,
            'area': 1,
            'is_europe': 2,
            'optimized': 3
        }


class RastrLoadScript(Script):
    def __init__(self):
        super().__init__('rastr_load.sql')
        self.index = {
            'hour': 0,
            'node_code': 1,
            'consumer_code': 2,
            'pn': 3,
            'node_dose': 4,
            'node_state': 5
        }


class RastrConsumerScript(Script):
    def __init__(self):
        super().__init__('rastr_consumer.sql')
        self.index = {
            'hour': 0,
            'consumer_code': 1,
            'type': 2,
            'pdem': 3
        }

class DisqualifiedDataScript(Script):
    def __init__(self):
        super().__init__('disqualified_data.sql')
        self.index = {
            'dpg_id': 0,
            'fed_station_cons': 1,
            'attached_supplies_gen': 2
        }


class RastrAreaScript(Script):
    def __init__(self):
        super().__init__('rastr_area.sql')
        self.index = {
            'hour': 0,
            'area': 1,
            'losses': 2,
            'load_losses': 3
        }


class LinesScript(Script):
    def __init__(self):
        super().__init__('lines.sql')
        self.index = {
            'hour': 0,
            'node_from': 1,
            'node_to': 2,
            'n_par': 3,
            'state': 4,
            'r': 5,
            'x': 6,
            'b': 7,
            'kt_re': 8,
            'kt_im': 9,
            'div': 10,
            'g': 11,
            'type': 12,
            'b_from': 13,
            'b_to': 14,
            'area': 15,
            'losses': 16
        }


class LinesScript_V(Script):
    def __init__(self):
        super().__init__('lines_v.sql')
        self.index = {
            'hour': 0,
            'node_from': 1,
            'node_to': 2,
            'n_par': 3,
            'state': 4,
            'r': 5,
            'x': 6,
            'b': 7,
            'kt_re': 8,
            'kt_im': 9,
            'div': 10,
            'g': 11,
            'type': 12,
            'b_from': 13,
            'b_to': 14,
            'area': 15,
            'losses': 16
        }


class RgeGroupsScript(Script):
    def __init__(self):
        super().__init__('rge_groups.sql')
        self.index = {
            'group_code': 0,
            'rge_code': 1
        }


class PReservesScript(Script):
    def __init__(self):
        super().__init__('p_reserves.sql')
        self.index = {
            'hour': 0,
            'group_code': 1,
            'state': 2,
            'p_min': 3,
            'p_max': 4
        }


class ImpexDpgsScript(Script):
    def __init__(self):
        super().__init__('impex_dpgs.sql')
        self.index = {
            'dpg_code': 0,
            'dpg_id': 1,
            'is_disqualified': 2,
            'is_unpriced_zone': 3,
            'section_number': 4,
            'direction': 5,
            'is_unpriced_zone': 6
        }


class SectionsScript(Script):
    def __init__(self):
        super().__init__('sections.sql')
        self.index = {
            'hour': 0,
            'section_code': 1,
            'p_max': 2,
            'p_min': 3,
            'state': 4,
            'type': 5
        }


class LineGroupsScript(Script):
    def __init__(self):
        super().__init__('rastr_grline.sql')
        self.index = {
            'hour': 0,
            'section_code': 1,
            'node_from_code': 2,
            'node_to_code': 3,
            'state': 4,
            'div': 5
        }


class GeneratorsLastHourScript(Script):
    """docstring for GeneratorsLastHourScript."""
    def __init__(self):
        super().__init__('generators_last_hour.sql')
        self.index = {
            'rge_code': 0,
            'volume': 1
        }


class SettingsScript(Script):
    """docstring for SettingsScript."""
    def __init__(self):
        super().__init__('settings.sql')
        self.index = {
            'code': 0,
            'value_type': 1,
            'string_value': 2,
            'number_value': 3,
            'target_date': 4
        }


class DGUsScript(Script):
    def __init__(self):
        super().__init__('dgus.sql')
        self.index = {
            'id': 0,
            'code': 1,
            'dpg_id': 2,
            'fixed_power': 3
        }


class DGUsScript_V(Script):
    def __init__(self):
        super().__init__('dgus_v.sql')
        self.index = {
            'id': 0,
            'code': 1,
            'dpg_id': 2,
            'fixed_power': 3
        }


class StationsScript(Script):
    """docstring for StationsScript."""
    def __init__(self):
        super().__init__('stations.sql')
        self.index = {
            'id': 0,
            'code': 1,
            'type': 2,
            'category': 3
        }


class StationsScript_V(Script):
    """docstring for StationsScript."""
    def __init__(self):
        super().__init__('stations_v.sql')
        self.index = {
            'id': 0,
            'code': 1,
            'type': 2,
            'category': 3
        }


class GUsScript(Script):
    """docstring"""
    def __init__(self):
        super().__init__('gu.sql')
        self.index = {
            'code': 0,
            'dgu_id': 1,
            'fuel_type_list': 2,
            'fixed_power': 3
        }

class GUsScript_V(Script):
    """docstring"""
    def __init__(self):
        super().__init__('gu_v.sql')
        self.index = {
            'code': 0,
            'dgu_id': 1,
            'fuel_type_list': 2,
            'fixed_power': 3
        }

class NBlockScript(Script):
    """rastr_nblock script"""
    def __init__(self):
        super().__init__('nblock.sql')
        self.index = {
            'hour': 0,
            'dgu_code': 1,
            'gu_code': 2,
            'pmin': 3,
            'pmax': 4,
            'pmin_t': 5,
            'pmax_t': 6,
            'state': 7,
            'vgain': 8,
            'vdrop': 9,
            'is_sysgen': 10,
            'repair': 11
        }


class NBlockScript_V(Script):
    """rastr_nblock script"""
    def __init__(self):
        super().__init__('nblock_v.sql')
        self.index = {
            'hour': 0,
            'dgu_code': 1,
            'gu_code': 2,
            'pmin': 3,
            'pmax': 4,
            'pmin_t': 5,
            'pmax_t': 6,
            'state': 7,
            'vgain': 8,
            'vdrop': 9,
            'is_sysgen': 10,
            'repair': 11
        }
