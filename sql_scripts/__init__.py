"""Script classes."""
import os
from collections import namedtuple
from operator import itemgetter

def make_index(keys):
    return keys
    # return {key: index for index, key in enumerate(keys.split())}


class Script(object):
    """base sql script class"""
    def __init__(self, filename):
        self.filename = filename
        self.query = ''
        self.index = {}
        self._read_file()
        self.Tup = None

    def __getitem__(self, item):
        return self.index[item]

    def _read_file(self):
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), self.filename)) as f:
            self.query = f.read()

    def _init_tuple(self):
        # fields = [item[0] for item in sorted([item for item in self.index.items()], key=itemgetter(1))]
        fields = self.index
        self.Tup = namedtuple(self.__class__.__name__ + '_namedtuple', fields)

    def get_query(self):
        return self.query


class GeneratorsScript(Script):
    def __init__(self, filename='generators.sql'):
        super().__init__(filename)
        self.index = make_index('''
            dpg_code
            gtp_id
            price_zone_code
            is_gaes
            is_blocked
            is_unpriced_zone
            fed_station_id
            station_id
            is_spot_trader
            dpg_demand_id
            region_code
            participant_id
        ''')
        self._init_tuple()

generators_script = GeneratorsScript()


class GeneratorsScript_V(GeneratorsScript):
    def __init__(self):
        super().__init__(r'Vertica\generators_vertica.sql')

generators_script_v = GeneratorsScript_V()


class BidInitsScript(Script):
    def __init__(self, filename='bid_init.sql'):
        super().__init__(filename)
        self.index = make_index('''
            dpg_code
            dpg_id
            bid_id
        ''')
        self._init_tuple()

bid_init_script = BidInitsScript()


class BidInitsScript_V(BidInitsScript):
    def __init__(self):
        super().__init__(r'Vertica\bid_init_vertica.sql')

bid_init_script_v = BidInitsScript_V()


class BidHoursScript(Script):
    def __init__(self, filename='bid_hour.sql'):
        super().__init__(filename)
        self.index = make_index('''
            bid_id
            bid_hour_id
            hour
            dpg_id
        ''')
        self._init_tuple()

bid_hour_script = BidHoursScript()


class BidHoursScript_V(BidHoursScript):
    def __init__(self):
        super().__init__(r'Vertica\bid_hour_vertica.sql')

bid_hour_script_v = BidHoursScript_V()


class BidPairsScript(Script):
    def __init__(self, filename='bid_pair.sql'):
        super().__init__(filename)
        self.index = make_index('''
            bid_hour_id
            interval_number
            price
            volume
            dpg_id
            volume_init
        ''')
        self._init_tuple()

bid_pair_script = BidPairsScript()


class BidPairsScript_V(BidPairsScript):
    def __init__(self):
        super().__init__(r'Vertica\bid_pair_vertica.sql')

bid_pair_script_v = BidPairsScript_V()


class RastrGenScript(Script):
    def __init__(self, filename='rastr_gen.sql'):
        super().__init__(filename)
        self.index = make_index('''
            hour
            rge_code
            pmin
            pmax
            pmin_agg
            pmax_agg
            pmin_tech
            pmax_tech
            pmin_heat
            pmax_heat
            pmin_so
            pmax_so
            p
            wmax
            wmin
            vgain
            vdrop
            node_code
        ''')
        self._init_tuple()

rastr_gen_script = RastrGenScript()


class RastrGenScript_V(RastrGenScript):
    def __init__(self):
        super().__init__(r'Vertica\dgus_hour_data_vertica.sql')

rastr_gen_script_v = RastrGenScript_V()


class WsumgenScript(Script):
    def __init__(self):
        super().__init__('wsumgen.sql')
        self.index = make_index('''
            rge_code
            price
            integral_id
            hour_start
            hour_end
            min_volume
            volume
            group_code
        ''')
        self._init_tuple()

wsumgen_script = WsumgenScript()


class ConsumersScript(Script):
    def __init__(self, filename='consumers.sql'):
        super().__init__(filename)
        self.index = make_index('''
            consumer_code
            area
            dem_rep_volume
            dem_rep_hours
            price_zone_code
            dpg_code
            is_system
            is_gp
            dpg_id
            is_fed_station
            is_disqualified
            is_unpriced_zone
            is_fsk
            min_forecast
            max_forecast
            is_spot_trader
            region_code
            participant_id
        ''')
        self._init_tuple()

consumers_script = ConsumersScript()


class ConsumersScript_V(ConsumersScript):
    def __init__(self):
        super().__init__(r'Vertica\consumers.sql')

consumers_script_v = ConsumersScript_V()


class MaxBidPriceScript(Script):
    def __init__(self):
        super().__init__('max_bid_price.sql')
        self.index = make_index('''
            hour
            max_bid_price
        ''')
        self._init_tuple()

max_bid_price_script = MaxBidPriceScript()


class NodesScript(Script):
    def __init__(self, filename='nodes.sql'):
        super().__init__(filename)
        self.index = make_index('''
            node_code
            area_code
            nominal_voltage
            min_voltage
            max_voltage
            price_zone
            hour
            state
            type
            pn
            max_q
            min_q
            voltage
            phase
            fixed_voltage
            g_shunt
            b_shunt
            qn
            qg
            pg
            q_shunt
            p_shunt
            b_shr
        ''')
        self._init_tuple()

nodes_script = NodesScript()


class NodesScript_V(NodesScript):
    def __init__(self):
        super().__init__(r'Vertica\nodes_vertica.sql')

nodes_script_v = NodesScript_V()


class ImpexAreaScript(Script):
    def __init__(self):
        super().__init__('impex_na.sql')
        self.index = make_index('''
            section_number
            area
            is_europe
            optimized
        ''')
        self._init_tuple()

impex_area_script = ImpexAreaScript()


class RastrLoadScript(Script):
    def __init__(self, filename='rastr_load.sql'):
        super().__init__(filename)
        self.index = make_index('''
            hour
            node_code
            consumer_code
            pn
            node_dose
            node_state
        ''')
        self._init_tuple()

rastr_load_script = RastrLoadScript()
rastr_load_script_v = RastrLoadScript(r'Vertica\load_vertica.sql')


class RastrConsumerScript(Script):
    def __init__(self, filename='rastr_consumer.sql'):
        super().__init__(filename)
        self.index = make_index('''
            hour
            consumer_code
            type
            pdem
            dem_rep_ready
        ''')
        self._init_tuple()

rastr_consumer_script = RastrConsumerScript()


class RastrConsumerScript_V(RastrConsumerScript):
    def __init__(self):
        super().__init__(r'Vertica\consumers_param.sql')

rastr_consumer_script_v = RastrConsumerScript_V()


class DisqualifiedDataScript(Script):
    def __init__(self):
        super().__init__('disqualified_data.sql')
        self.index = make_index('''
            dpg_id
            dpg_code
            fed_station_cons
            attached_supplies_gen
        ''')
        self._init_tuple()

disqualified_data_script = DisqualifiedDataScript()


class RastrAreaScript(Script):
    def __init__(self, filename='rastr_area.sql'):
        super().__init__(filename)
        self.index = make_index('''
            hour
            area
            losses
            load_losses
        ''')
        self._init_tuple()

rastr_areas_script = RastrAreaScript()


class RastrAreaScript_V(RastrAreaScript):
    def __init__(self):
        super().__init__(r'Vertica\area_vertica.sql')

rastr_areas_script_v = RastrAreaScript_V()


class LinesScript(Script):
    def __init__(self, filename='lines.sql'):
        super().__init__(filename)
        self.index = make_index('''
            node_from
            node_to
            n_par
            kt_re
            kt_im
            div
            type
            area_code
            hour
            state
            r
            x
            b
            g
            b_from
            b_to
            losses
        ''')
        self._init_tuple()

lines_script = LinesScript()


class LinesScript_V(LinesScript):
    def __init__(self):
        super().__init__(r'Vertica\lines_vertica.sql')

lines_script_v = LinesScript_V()


class RgeGroupsScript(Script):
    def __init__(self):
        super().__init__('rge_groups.sql')
        self.index = make_index('''
            group_code
            rge_code
        ''')
        self._init_tuple()

rge_groups_script = RgeGroupsScript()


class PReservesScript(Script):
    def __init__(self):
        super().__init__('p_reserves.sql')
        self.index = make_index('''
            hour
            group_code
            state
            p_min
            p_max
        ''')
        self._init_tuple()

preserves_script = PReservesScript()


class ImpexDpgsScript(Script):
    def __init__(self):
        super().__init__('impex_dpgs.sql')
        self.index = make_index('''
            dpg_code
            dpg_id
            is_disqualified
            is_unpriced_zone
            section_number
            direction
            is_spot_trader
            region_code
            price_zone_code
            participant_id
        ''')
        self._init_tuple()

impex_dpgs_script = ImpexDpgsScript()


class SectionsScript(Script):
    def __init__(self, filename='sections.sql'):
        super().__init__(filename)
        self.index = make_index('''
            hour
            code
            p_max
            p_min
            state
            type
        ''')
        self._init_tuple()

sections_script = SectionsScript()


class SectionsScript_V(SectionsScript):
    def __init__(self):
        super().__init__(r'Vertica\sections.sql')

sections_script_v = SectionsScript_V()


class LineGroupsScript(Script):
    def __init__(self, filename='rastr_grline.sql'):
        super().__init__(filename)
        self.index = make_index('''
            hour
            section_code
            node_from_code
            node_to_code
            state
            div
        ''')
        self._init_tuple()

line_groups_script = LineGroupsScript()


class LineGroupsScript_V(LineGroupsScript):
    def __init__(self):
        super().__init__(r'Vertica\rastr_grline.sql')

line_groups_script_v = LineGroupsScript_V()


class GeneratorsLastHourScript(Script):
    def __init__(self):
        super().__init__('generators_last_hour.sql')
        self.index = make_index('''
            rge_code
            volume
        ''')
        self._init_tuple()

generators_last_hour_script = GeneratorsLastHourScript()


class SettingsScript(Script):
    def __init__(self):
        super().__init__('settings.sql')
        self.index = make_index('''
            code
            value_type
            string_value
            number_value
            target_date
        ''')
        self._init_tuple()

settings_script = SettingsScript()


class PeakSOScript(Script):
    def __init__(self):
        super().__init__('peak_so.sql')
        self.index = make_index('''
            price_zone_code
            hour_start
            hour_end
        ''')
        self._init_tuple()

peak_so_script = PeakSOScript()


class DGUsScript(Script):
    def __init__(self, filename='dgus.sql'):
        super().__init__(filename)
        self.index = make_index('''
            id
            code
            dpg_id
            fixed_power
        ''')
        self._init_tuple()

dgus_script = DGUsScript()


class DGUsScript_V(DGUsScript):
    def __init__(self):
        super().__init__(r'Vertica\dgus_vertica.sql')

dgus_script_v = DGUsScript_V()


class StationsScript(Script):
    def __init__(self, filename='stations.sql'):
        super().__init__(filename)
        self.index = make_index('''
            id
            code
            type
            category
        ''')
        self._init_tuple()

stations_script = StationsScript()


class StationsScript_V(StationsScript):
    def __init__(self):
        super().__init__(r'Vertica\stations_vertica.sql')

stations_script_v = StationsScript_V()


class GUsScript(Script):
    """docstring"""
    def __init__(self, filename='gu.sql'):
        super().__init__(filename)
        self.index = make_index('''
            id
            code
            dgu_id
            fuel_type_list
            fixed_power
        ''')
        self._init_tuple()

gus_script = GUsScript()


class GUsScript_V(GUsScript):
    def __init__(self):
        super().__init__(r'Vertica\gu_vertica.sql')

gus_script_v = GUsScript_V()


class NBlockScript(Script):
    def __init__(self, filename='nblock.sql'):
        super().__init__(filename)
        self.index = make_index('''
            hour
            dgu_code
            gu_code
            pmin
            pmax
            pmin_t
            pmax_t
            state
            vgain
            vdrop
            is_sysgen
            repair
        ''')
        self._init_tuple()

nblock_script = NBlockScript()


class NBlockScript_V(NBlockScript):
    def __init__(self):
        super().__init__(r'Vertica\gu_hour_data_vertica.sql')

nblock_script_v = NBlockScript_V()


class BlockOut(Script):
    def __init__(self):
        super().__init__(r'Vertica\gu_out.sql')
        self.index = make_index('gu_code')
        self._init_tuple()

block_out_v = BlockOut()


class PriceZoneScript(Script):
    def __init__(self):
        super().__init__('price_zones.sql')
        self.index = make_index('''
            price_zone_code
            power_consumption
        ''')
        self._init_tuple()

price_zone_script = PriceZoneScript()


class HydroNewVolumeScript(Script):
    def __init__(self):
        super().__init__(r'Vertica\ges_new_pdg.sql')
        self.index = make_index('''
            dgu_code
            hour
            volume
        ''')
        self._init_tuple()

hydro_new_volume_script = HydroNewVolumeScript()
