"""Create Bid instances."""
import re
from operator import attrgetter
from utils import DB
from utils.trade_session_manager import ts_manager
from utils.progress_bar import update_progress
from sql_scripts import bid_init_script as bis, bid_hour_script as bhs, bid_pair_script as bps, \
bid_init_script_v as bis_v, bid_hour_script_v as bhs_v, bid_pair_script_v as bps_v, \
bid_factor_script as bfs
from .bids import Bid


HOURCOUNT = 24

@ts_manager
def make_bids(tsid):
    """create Bid instances"""
    con = DB.OracleConnection()
    Bid.clear()

    for new_row in con.script_cursor(bis, tsid=DB.Partition(tsid)):
        Bid(new_row)

    for new_row in con.script_cursor(bhs, tsid=DB.Partition(tsid)):
        bid = Bid[new_row.dpg_id]
        if bid:
            bid.add_hour_data(new_row)

    for new_row in con.script_cursor(bps, tsid=DB.Partition(tsid)):
        bid = Bid[new_row.dpg_id]
        if bid:
            bid.add_intervals_data(new_row)


@ts_manager
def add_bids_vertica(scenario):
    """add Bid instances from Vertica DB"""
    con = DB.VerticaConnection()

    for new_row in con.script_cursor(bis_v, scenario=scenario):
        Bid(new_row, is_new=True)


    for new_row in con.script_cursor(bhs_v, scenario=scenario):
        bid = Bid[new_row.dpg_id]
        if bid:
            bid.add_hour_data(new_row)


    # h_re = re.compile(r'(?<=_)\d+')
    for new_row in con.script_cursor(bps_v, scenario=scenario):
        bid = Bid[new_row.dpg_id]
        if bid:
            bid.add_intervals_data(new_row)

@ts_manager
def send_bids_to_db(ora_con, tdate):
    """save new instances to current session"""
    bid_init_insert = []
    bid_init_hour_insert = []
    bid_init_pair_insert = []
    dpgs = []
    for bid in Bid:
        if bid.is_new:
            dpgs.append((bid.dpg_id,))
            bid_init_insert.append((
                bid.dpg_code, bid.dpg_id, bid.bid_id, tdate
            ))
            for bih in bid:
                bid_init_hour_insert.append((
                    bih.bid_id, bih.bid_hour_id, bih.hour, bid.dpg_id
                ))
                for bip in bih.interval_data:
                    bid_init_pair_insert.append((
                        bip.bid_hour_id, bip.interval_number, bip.price,
                        bip.volume, bid.dpg_id, bip.volume_init
                    ))

    with ora_con.cursor() as curs:
        curs.executemany('DELETE from bid_init_pair where dpg_id = :1', dpgs)
        curs.executemany('DELETE from bid_init_hour where dpg_id = :1', dpgs)
        curs.executemany('DELETE from bid_init where dpg_id = :1', dpgs)
        curs.executemany('''INSERT into bid_init (dpg_code, dpg_id, bid_id, target_date)
                               values (:1, :2, :3, :4)''', bid_init_insert)
        curs.executemany('''INSERT into bid_init_hour (bid_id, bid_hour_id, hour, dpg_id)
                            values (:1, :2, :3, :4)''', bid_init_hour_insert)
        curs.executemany('''INSERT into bid_init_pair (bid_hour_id, interval_num,
                            price, volume, dpg_id, volume_src0)
                            values (:1, :2, :3, :4, :5, :6)''', bid_init_pair_insert)
