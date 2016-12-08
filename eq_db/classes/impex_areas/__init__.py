import time
from utils import DB, ORM
from utils.progress_bar import update_progress

from sql_scripts import impex_area_script as ias
from .impex_areas import ImpexArea


def make_impex_areas(tsid={}, tdate=''):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}
    print('making impex_areas%s' % ((' for date %s' % tdate) if tdate else ''))

    start_time = time.time()

    con = DB.OracleConnection()

    @DB.process_cursor(con, ias, tsid)
    def process_impex_areas(new_row, impex_areas_list):
        impex_areas_list.append(ImpexArea(new_row))

    impex_areas = []
    process_impex_areas(impex_areas)

    # dpgs.finalize_data()

    # ORM.session.add_all(dpgs.dpg_list)
    for i, impex_area in enumerate(impex_areas):
        impex_area.serialize(ORM.session)
        update_progress((i + 1) / len(impex_areas))
    ORM.session.commit()

    print('%s %s seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))

    return impex_areas
