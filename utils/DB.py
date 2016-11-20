import vertica_python
import cx_Oracle
import settings
from utils.progress_bar import RollerBar
import re
import copy


def process_cursor(connection, script, input_data={}):
    def decorator(fn):
        def dummy(*args):
            bar = RollerBar()
            cursor = connection.get_script_cursor(script.get_query(), input_data)
            row = cursor.fetchone()
            while row:
                fn(row, *args)
                row = cursor.fetchone()
                bar.roll()
            bar.finish()
            cursor.close()
        return dummy
    return decorator


class _DBConnection(object):
    def __init__(self):
        self.con = None

    def __del__(self):
        # print("closing connection...")
        self.con.close()
        # print("done!")

    def get_script_cursor(self, query, input_data={}):
        query, input_data = self._replace_trade_session_id(query, input_data)

        curs = self.con.cursor()
        curs.execute(query, input_data)

        return curs

    def exec_script(self, query, input_data={}):
        db_res = []

        # for key in input_data.keys():
        #     query = query.replace('&' + key, str(input_data[key]))
        query, input_data = self._replace_trade_session_id(query, input_data)

        curs = self.con.cursor()
        curs.execute(query, input_data)

        for row in curs.fetchall():
            db_res.append(row)

        curs.close()
        return db_res

    @staticmethod
    def _replace_trade_session_id(query, input_data_src):
        input_data = copy.deepcopy(input_data_src)
        if input_data:
            if 'tsid' in input_data.keys():
                query = query.replace('&tsid', 'TS_' + str(input_data['tsid']))
                del input_data['tsid']
        else:
            for match in re.finditer('(wh_(\w*)\s*partition\s*\(&tsid\))', query):
                query = query.replace(match.group(1), match.group(2))

        return query, input_data


class OracleConnection(_DBConnection):
    def __init__(self):
        self.con = cx_Oracle.connect(settings.oracle_connection_string)


class VerticaConnection(_DBConnection):
    def __init__(self):
        self.con = vertica_python.connect(**settings.vertica_connection_opts)
