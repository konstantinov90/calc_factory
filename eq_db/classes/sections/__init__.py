"""Create Section instances."""
from utils import DB
from utils.trade_session_manager import ts_manager
from sql_scripts import sections_script as ss
from sql_scripts import line_groups_script as lgs

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
