"""Create Section instances."""
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import sections_script as ss
from sql_scripts import sections_script_v as ss_v
from sql_scripts import line_groups_script as lgs
from sql_scripts import line_groups_script_v as lgs_v

from .sections import Section


@ts_manager
def make_sections(tsid):
    """create Section instances"""
    Section.clear()
    con = DB.OracleConnection()

    for new_row in con.script_cursor(ss, tsid=tsid):
        section_code = new_row.code
        section = Section[section_code]
        if not section:
            section = Section(new_row)
        section.add_section_hour_data(new_row)

    for new_row in con.script_cursor(lgs, tsid=tsid):
        section_code = new_row.section_code
        section = Section[section_code]
        if section:
            section.add_section_hour_line_data(new_row)

@ts_manager
def add_sections_vertica(scenario, **kwargs):
    """add Section instances from Vertica DB"""
    con = DB.VerticaConnection()
    ora_con = kwargs['ora_con']
    tdate = kwargs['target_date']

    for new_row in con.script_cursor(ss_v, scenario=scenario):
        section_code = new_row.code
        section = Section[section_code]
        if not section:
            section = Section(new_row)
        section.add_section_hour_data(new_row)

    for new_row in con.script_cursor(lgs_v, scenario=scenario):
        section_code = new_row.section_code
        section = Section[section_code]
        if section:
            section.add_section_hour_line_data(new_row)
