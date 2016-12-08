import time
from utils import DB, ORM
from utils.progress_bar import update_progress

from sql_scripts import wsumgen_script as ws
from .wsumgen import Wsumgen


def make_wsumgen(tsid={}, tdate=''):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}
    print('making wsumgen%s' % ((' for date %s' % tdate) if tdate else ''))

    start_time = time.time()

    con = DB.OracleConnection()
    
    @DB.process_cursor(con, ws, tsid)
    def process_wsumgen(new_row, wsumgen_list):
        wsumgen_list.append(Wsumgen(new_row))

    ws_lst = []
    process_wsumgen(ws_lst)

    for i, wsumgen in enumerate(ws_lst):
        wsumgen.serialize(ORM.session)
        update_progress((i + 1) / len(ws_lst))
    ORM.session.commit()

    print('%s %s seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))

    return wsumgen
