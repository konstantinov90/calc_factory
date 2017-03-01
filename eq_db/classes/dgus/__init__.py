"""Create Dgu instances."""
from operator import attrgetter
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import dgus_script as dgs
from sql_scripts import dgus_script_v as dgs_v
from sql_scripts import rastr_gen_script as rgs
from sql_scripts import rastr_gen_script_v as rgs_v
from sql_scripts import generators_last_hour_script as glhs
from sql_scripts import hydro_new_volume_script as hnvs
from .dgus import Dgu

DGU_TRADER_TYPE = 103


@ts_manager
def make_dgus(tsid):
    """create Dgu instances"""
    con = DB.OracleConnection()

    for new_row in con.script_cursor(dgs, tsid=tsid):
        Dgu(new_row)

    for new_row in con.script_cursor(rgs, tsid=tsid):
        dgu = Dgu.by_code[new_row.rge_code]
        if dgu:
            dgu.add_dgu_hour_data(new_row)

    for new_row in con.script_cursor(glhs, tsid=tsid):
        dgu = Dgu.by_code[new_row.rge_code]
        if dgu:
            dgu.set_last_hour(new_row)

@ts_manager
def add_dgus_vertica(scenario):
    """add Dgu instances from Vertica DB"""
    con = DB.VerticaConnection()

    for new_row in con.script_cursor(dgs_v, scenario=scenario):
        print(Dgu(new_row, is_new=True))

    for new_row in con.script_cursor(rgs_v, scenario=scenario):
        dgu = Dgu.by_code[new_row.rge_code]
        if dgu:
            dgu.add_dgu_hour_data(new_row)

    for new_row in con.script_cursor(hnvs, scenario=scenario):
        dgu = Dgu.by_code[new_row.dgu_code]
        if not dgu:
            raise Exception('vertica changes volume for nonexistent dgu %i' % new_row.dgu_code)
        dgu_hd = dgu.hour_data[new_row.hour]
        dgu_hd.p = new_row.volume
        dgu_hd.pmax = max(new_row.volume, dgu_hd.pmax)

@ts_manager
def send_dgus_to_db(ora_con, tdate):
    """save new instances to current session"""
    data = []
    attrs = '_id _id code dpg_id fixed_power'.split()
    atg = attrgetter(*attrs)
    const_part = (tdate, tdate, DGU_TRADER_TYPE)
    attrs_len = len(attrs) + len(const_part)
    h_data = []
    hour_attrs = '''hour pmin pmax pmin_agg pmax_agg pmin_tech pmax_tech
                 pmin_heat pmax_heat pmin_so pmax_so p wmax wmin
                 vgain vdrop'''.split()
    hour_atg = attrgetter(*hour_attrs)
    hour_attrs_len = len(hour_attrs) + 2
    for dgu in Dgu:
        if dgu.is_new:
            data.append(
                atg(dgu) + const_part
            )
        for _hd in dgu.hour_data:
            h_data.append(
                hour_atg(_hd) + (dgu.code, dgu.node_code)
            )

    with ora_con.cursor() as curs:
        curs.execute('''
            DELETE from trader
            where start_version is null
            and trader_type = :type
        ''', type=DGU_TRADER_TYPE)
        curs.execute('''
            DELETE from rastr_generator
        ''')

        curs.executemany('''
            INSERT into trader (trader_id, real_trader_id, trader_code,
            parent_object_id, fixed_power, begin_date, end_date, trader_type)
            values(:{})
        '''.format(', :'.join(str(i + 1) for i in range(attrs_len))),
                    data)

        curs.executemany('''
            INSERT into rastr_generator (hour, o$pmin, o$pmax, o$pminagg, o$pmaxagg,
            o$dpmintech, o$dpmaxtech, o$dpminheat, o$dpmaxheat, o$dpminso, o$dpmaxso,
            o$p, o$wmax, o$wmin, o$vgain, o$vdrop, o$num, o$node)
            values(:{})
        '''.format(', :'.join(str(i + 1) for i in range(hour_attrs_len))),
                    h_data)
