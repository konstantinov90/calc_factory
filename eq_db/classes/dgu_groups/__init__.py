"""Create DguGroup instances."""
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import rge_groups_script as rgs
from sql_scripts import preserves_script as prs

from .dgu_groups import DguGroup


@ts_manager
def make_dgu_groups(tsid):
    """create DguGroup instances"""
    con = DB.OracleConnection()
    DguGroup.clear()

    for new_row in con.script_cursor(rgs, tsid=DB.Partition(tsid)):
        dgu_group = DguGroup[new_row.group_code]
        if not dgu_group:
            dgu_group = DguGroup(new_row)
        dgu_group.add_dgu(new_row)

    for new_row in con.script_cursor(prs, tsid=DB.Partition(tsid)):
        dgu_group = DguGroup[new_row.group_code]
        if dgu_group:
            dgu_group.add_reserve_data(new_row)
