"""Class Area."""
from operator import attrgetter
from ..meta_base import MetaBase
from .areas_hour_data import AreaHourData

HOURCOUNT = 24

class Area(object, metaclass=MetaBase):
    """class Area"""
    def __init__(self, ra_row):
        self.code = int(ra_row.area)
        self.area_hour_data = {}
        self.nodes = None
        self.impex_data = None
        self.dpg = None
        self._init_on_load()

    @property
    def hour_data(self):
        """hour_data property"""
        return sorted(self.area_hour_data.values(), key=attrgetter('hour'))

    @property
    def p_n(self):
        """sum p_n of all nodes"""
        return sum(node.p_n for node in self.nodes)

    def __repr__(self):
        return '<Area %i>' % self.code

    def _init_on_load(self):
        """additional initialization"""
        if self.code not in self.lst.keys():
            self.lst[self.code] = self

    def add_area_hour_data(self, ra_row):
        """add AreaHourData instance"""
        hour = ra_row.hour
        if hour in self.area_hour_data.keys():
            raise Exception('tried to add area %i hour twice!' % self.code)
        self.area_hour_data[hour] = AreaHourData(ra_row)

    def get_sum_pn_retail_diff(self, hour):
        """
        get sum of difference between node.pn and node.retail
        in case of negative sum return zero
        """
        def func(node):
            """aux function"""
            if node.get_node_hour_state(hour):
                return max(node.hour_data[hour].pn - node.hour_data[hour].retail, 0)
            else:
                return 0

        return sum(func(node) for node in self.nodes)

    def get_nodes_on(self, hour):
        """return number of turned on nodes in this area at particular hour"""
        return sum(1 if node.get_node_hour_state(hour) else 0 for node in self.nodes)

    def attach_nodes(self, nodes_list):
        """attach Node instances and set node's area and dpg properties"""
        self.nodes = \
            [node.set_area_and_ret(self) for node in nodes_list if node.area_code == self.code]
        for node in self.nodes:
            node.set_dpg(self.dpg)

    def set_impex_data(self, impex_areas_list):
        """set impex_data if any corresponding ImpexArea instance"""
        dummy = [ia for ia in impex_areas_list if ia.area_code == self.code]
        if len(dummy) > 1:
            raise Exception('too many impex areas for area %i' % self.code)
        if dummy:
            self.impex_data = dummy[0]

    def fill_db(self, con):
        """fill kc_dpg_node"""
        script = """INSERT into kc_dpg_node (hour, node, kc, sta, dpg_id, is_system,
                                dpg_code, consumer2)
                    VALUES (:1, :2, :3, :4, :5, :6, :7, :8)"""
        data = []
        for node_data in self.nodes:
            for _hd in node_data.hour_data:
                data.append(_hd.get_insert_data() + attrgetter('_id', 'is_system', 'code', 'consumer_code')(self.dpg))
        with con.cursor() as curs:
            curs.executemany(script, data)
