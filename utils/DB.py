"""DB module contains DBConnection classes and process_cursor decorator."""
# import decimal
import re
# import copy
from contextlib import contextmanager
import vertica_python
import cx_Oracle
import settings
from sql_scripts import Script
from utils.progress_bar import RollerBar

ARRAYSIZE = 1000

# def process_cursor(connection, script, **input_data):
#     """process_cursor decorator factory"""
#     def decorator(fun):
#         """decorator function"""
#         def dummy(*args):
#             """function to return"""
#             roller = RollerBar()
#             cursor = connection.get_script_cursor(script.get_query(), input_data)
#             row = cursor.fetchone()
#             while row:
#                 fun(row, *args)
#                 row = cursor.fetchone()
#                 roller.roll()
#             roller.finish()
#             cursor.close()
#         return dummy
#     return decorator


class _DBConnection(object):
    """base DBConnection class"""
    def __init__(self):
        self.con = None
        self.curs = None

    def __del__(self):
        # release connection
        # print("closing connection...")
        self.con.close()
        # print("done!")

    @staticmethod
    def _replace_trade_session_id(query, input_data):
        """replace trade_session_id in query"""
        # input_data = copy.deepcopy(input_data_src)
        try:
            tsid = input_data['tsid']
            del input_data['tsid']
        except KeyError:
            tsid = None

        if tsid:
            query = query.replace('&tsid', 'TS_' + str(input_data['tsid']))
        else:
            for match in re.finditer(r'(wh_(\w*)\s*partition\s*\(&tsid\))',
                                     query, flags=re.IGNORECASE):
                query = query.replace(match.group(1), match.group(2))
        return query, input_data

    def commit(self):
        """commit sql transaction"""
        self.con.commit()

    def rollback(self):
        """rollback sql transaction"""
        self.con.rollback()

    def get_script_cursor(self, query, input_data):
        """get sql query cursor"""
        curs = self.con.cursor()
        curs.execute(*self._replace_trade_session_id(query, input_data))
        return curs

    def script_cursor(self, script, **input_data):
        """get cursor generator"""
        roller = RollerBar()

        if isinstance(script, Script):
            query = script.get_query()
            tup = script.Tup if script.Tup else tuple
        else:
            query = script
            tup = tuple

        cursor = self.get_script_cursor(query, input_data)

        while True:
            rows = cursor.fetchmany(ARRAYSIZE)
            if not rows:
                break
            for row in rows:
                try:
                    yield tup(*row)
                except TypeError:
                    yield tup(row)
                roller.roll()
        roller.finish()
        cursor.close()

    @contextmanager
    def cursor(self):
        """get cursor as context"""
        self.curs = self.con.cursor()
        yield self.curs
        self.curs.close()

    def exec_insert(self, query, **input_data):
        """execute pl/sql expression"""
        curs = self.con.cursor()
        curs.execute(*self._replace_trade_session_id(query, input_data))
        curs.close()

    def exec_script(self, query, **input_data):
        """execute query and get results as a list"""
        db_res = []

        curs = self.con.cursor()
        curs.execute(*self._replace_trade_session_id(query, input_data))

        for row in curs.fetchall():
            db_res.append(row)

        curs.close()
        return db_res

# def OraTypeHandler(cursor, name, defaultType, size, precision, scale):
#     if defaultType == cx_Oracle.NUMBER:
#         return cursor.var(str, 100, cursor.arraysize, outconverter=decimal.Decimal)

class OracleConnection(_DBConnection):
    """This class establishes connection to ORACLE DataBase."""
    def __init__(self, conn_str=settings.oracle_connection_string):
        super().__init__()
        self.con = cx_Oracle.connect(conn_str)
        # self.con.outputtypehandler = OraTypeHandler


class VerticaConnection(_DBConnection):
    """This class establishes connection to Vertica DataBase."""
    def __init__(self):
        super().__init__()
        self.con = vertica_python.connect(**settings.vertica_connection_opts)
