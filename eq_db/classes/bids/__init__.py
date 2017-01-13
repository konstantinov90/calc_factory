"""Create Bid instances."""
from operator import attrgetter
from utils import DB
from utils.trade_session_manager import ts_manager
from utils.progress_bar import update_progress
from sql_scripts import bid_init_script as bis
from sql_scripts import bid_hour_script as bhs
from sql_scripts import bid_pair_script as bps
from sql_scripts import bid_init_script_v as bis_v
from sql_scripts import bid_hour_script_v as bhs_v
from sql_scripts import bid_pair_script_v as bps_v
from .bids import Bid


HOURCOUNT = 24

@ts_manager
def make_bids(tsid):
    """create Bid instances"""
    con = DB.OracleConnection()
    Bid.clear()

    for new_row in con.script_cursor(bis, tsid=tsid):
        Bid(new_row)

    for new_row in con.script_cursor(bhs, tsid=tsid):
        bid = Bid[new_row.dpg_id]
        if bid:
            bid.add_hour_data(new_row)

    for new_row in con.script_cursor(bps, tsid=tsid):
        bid = Bid[new_row.dpg_id]
        if bid:
            bid.add_intervals_data(new_row)


@ts_manager
def add_bids_vertica(scenario, target_date):
    """add Bid instances from Vertica DB"""
    con = DB.VerticaConnection()

    for new_row in con.script_cursor(bis_v, scenario=scenario, target_date=target_date):
        Bid(new_row)

    for new_row in con.script_cursor(bhs_v, scenario=scenario):
        bid = Bid[new_row.dpg_id]
        if bid:
            bid.add_hour_data(new_row)

    for new_row in con.script_cursor(bps_v, scenario=scenario):
        bid = Bid[new_row.dpg_id]
        if bid:
            bid.add_intervals_data(new_row)
