"""Create Station instances."""
from utils import DB
from utils.printer import print
from utils.trade_session_manager import ts_manager
from sql_scripts import stations_script as ss
from sql_scripts import stations_script_v as ss_v
from .stations import Station

STATION_TRADER_TYPE = 102

@ts_manager
def make_stations(tsid):
    """create Station instances"""
    con = DB.OracleConnection()
    Station.clear()

    for new_row in con.script_cursor(ss, tsid=DB.Partition(tsid)):
        Station(new_row)

@ts_manager
def add_stations_vertica(scenario):
    """add Station instances from Vertica DB"""
    con = DB.VerticaConnection()
    # ora_con = kwargs['ora_con']

    for new_row in con.script_cursor(ss_v, scenario=scenario):
        if Station[new_row.id]:
            # print('vertica contains already existing station %s' % new_row.code)
            # continue
            raise Exception('vertica contains already existing station %s' % new_row.code)

        # if not ora_con.exec_script('''
        #             select trader_id from trader where trader_code=:trader_code
        #         ''', trader_code=new_row.code):
        #     ora_con.exec_insert('''
        #         insert into trader(trader_id, real_trader_id, trader_code, station_type,
        #         station_category, begin_date, end_date, trader_type)
        #         values(:id, :id, :code, :type, :category, :tdate, :tdate, :trader_type)
        #         ''', tdate=kwargs.get('target_date'),
        #             trader_type=STATION_TRADER_TYPE, **new_row._asdict())
        print(Station(new_row, is_new=True))

@ts_manager
def send_stations_to_db(ora_con, tdate):
    """save new instances to current session"""
    data = []
    for station in Station:
        if station.is_new:
            data.append((
                station._id, station._id, station.code, station.type,
                station.category, tdate, tdate, STATION_TRADER_TYPE,
                station.participant_id
            ))

    with ora_con.cursor() as curs:
        curs.execute('''
            DELETE from trader
            where start_version is null
            and trader_type = :type
        ''', type=STATION_TRADER_TYPE)

        curs.executemany('''
            INSERT into trader (trader_id, real_trader_id, trader_code, station_type,
            station_category, begin_date, end_date, trader_type, parent_object_id)
            values (:1, :2, :3, :4, :5, :6, :7, :8, :9)
        ''', data)
