"""Create Bid instances."""
import re
from operator import attrgetter
from utils import DB
from utils.trade_session_manager import ts_manager
from utils.progress_bar import update_progress
from sql_scripts import bid_init_script as bis, bid_hour_script as bhs, bid_pair_script as bps, \
bid_init_script_v as bis_v, bid_hour_script_v as bhs_v, bid_pair_script_v as bps_v
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
def add_bids_vertica(scenario, **kwargs):
    """add Bid instances from Vertica DB"""
    con = DB.VerticaConnection()
    ora_con = kwargs['ora_con']
    target_date = kwargs['target_date']

    bid_init_insert = []
    for new_row in con.script_cursor(bis_v, scenario=scenario, target_date=target_date):
        dpg_id = new_row.dpg_id
        dpg_code = ora_con.exec_script('''
            SELECT dpg_code from bid_init where dpg_id = :dpd
        ''', dpd=dpg_id)
        if dpg_code:
            if dpg_code != [(new_row.dpg_code,)]:
                raise Exception('something wrong with bid %s' % new_row.dpg_code)
            # ora_con.exec_insert('DELETE from bid_init_pair where dpg_id = :dpd', dpd=dpg_id)
            # ora_con.exec_insert('DELETE from bid_init_hour where dpg_id = :dpd', dpd=dpg_id)
            # ora_con.exec_insert('DELETE from bid_init where dpg_id = :dpd', dpd=dpg_id)

        # ora_con.exec_insert('''INSERT into bid_init (bid_id, dpg_code, dpg_id, target_date)
        #                        values (:bid_id, :dpg_code, :dpg_id, :tdate)''',
        #                     tdate=target_date, **new_row._asdict())
        bid_init_insert.append(new_row + (target_date,))

        Bid(new_row, True)


    bid_init_hour_insert = []
    for new_row in con.script_cursor(bhs_v, scenario=scenario):
        bid = Bid[new_row.dpg_id]
        if bid:
            # [(bid_id,)] = ora_con.exec_script('''SELECT bid_id from bid_init
            #                                      where dpg_id = :dpd''', dpd=new_row.dpg_id)
            # try:
            #     ora_con.exec_insert('''INSERT into bid_init_hour (bid_hour_id, bid_id, hour, dpg_id)
            #                            values (:bid_hour_id, :bid_id, :hour, :dpg_id)''',
            #                         **new_row._asdict())
            # except Exception:
            #     print(new_row)
            #     raise
            bid_init_hour_insert.append(tuple(new_row))
            bid.add_hour_data(new_row)


    # h_re = re.compile(r'(?<=_)\d+')
    bid_init_pair_insert = []
    for new_row in con.script_cursor(bps_v, scenario=scenario):
        bid = Bid[new_row.dpg_id]
        if bid:
            # hour = int(h_re.search(new_row.bid_hour_id).group(0))
            # [(bid_hour_id,)] = ora_con.exec_script('''SELECT bid_hour_id from bid_init_hour
            #                                           where dpg_id = :dpd and hour = :hour''',
            #                                        dpd=new_row.dpg_id, hour=hour)

            # ora_con.exec_insert('''INSERT into bid_init_pair (bid_hour_id, interval_num,
            #                        price, volume, dpg_id, volume_src0)
            #                     values (:bid_hour_id, :interval_number, :price, :volume,
            #                             :dpg_id, :volume_init)''', **new_row._asdict())
            bid_init_pair_insert.append(tuple(new_row))
            bid.add_intervals_data(new_row)

    dpgs = [(b[1],) for b in bid_init_insert]
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
