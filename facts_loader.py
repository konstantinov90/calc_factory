import os
import csv
import datetime
from operator import itemgetter

from utils import DB

tbl_script = """
    SELECT table_name, status
    from all_tables
    where owner = 'FACTS'
    and table_name = 'LOADBIDS_DPGG_DATE'
"""
col_script = """
    SELECT column_name, data_type
    from all_tab_columns
    where table_name = :tab_name
"""


con = DB.OracleConnection('facts/facts@ts_black')

# Заполнение таблиц FACTS из файлов-INSERT'ов
#
# def read2(f):
#     for line in f:
#         try:
#             line2 = next(f)
#         except StopIteration:
#             line2 = ''
#
#         yield '%s %s' % (line , line2.replace(';', ''))
#
# with con as curs:
#
#     for table_name, stts in con.exec_script(tbl_script):
#         script = 'truncate table %s' % table_name
#         print(script)
#         curs.execute(script)
#
#     for scriptname in os.listdir(r'C:\work\model\data'):
#         with open('C:\\work\\model\\data\\%s' % scriptname, 'r') as f:
#             for script in read2(f):
#                 curs.execute(script)
#                 con.commit()

# Формирование csv-файлов из таблиц FACTS
class Query(object):
    """вспомогательный класс для декоратора @DB.process_cursor"""
    def __init__(self, query):
        self.query = query

    def get_query(self):
        return self.query

for table_name, status in con.exec_script(tbl_script):
    cols = con.exec_script(col_script, {'tab_name': table_name})

    @DB.process_cursor(con, Query('select %s from %s' % (', '.join(map(itemgetter(0), cols)), table_name)))
    def process_table(new_row, _file):
        print(';'.join(map(str,new_row)), file=_file)

    with open('facts\\%s.csv' % table_name, 'w') as f:
        print(';'.join(map(itemgetter(0), cols)), file=f)
        process_table(f)


# Заполнение таблиц FACTS из csv-файлов
# def value_handler(x):
#     if x == 'None':
#         return None
#     try:
#         return datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').date()
#     except ValueError:
#         try:
#             return int(x)
#         except ValueError:
#             try:
#                 return float(x)
#             except ValueError:
#                 return x
#
# for table_name, status in con.exec_script(tbl_script):
#     print(table_name)
#     with open('facts\\%s.csv' % table_name, 'r') as f:
#         reader = csv.reader(f, delimiter=';', quotechar='"')
#         cols = next(reader)
#         prepared_statement = 'INSERT INTO %s (%s) VALUES (:%s)' \
#                               % (table_name,
#                                  ', '.join(cols),
#                                  ', :'.join(map(lambda x: str(x+1), range(len(cols))))
#                                 )
#         print(cols)
#
#         rows = []
#         for row in reader:
#             rw = list(map(value_handler, row))
#             if table_name == 'LOADCRM_GU_MONTH':
#                 rw = rw[:5] + [str(rw[5])] + rw[6:]
#             rows += (rw,)
#         if not rows:
#             continue
#
#         with con as curs:
#             curs.execute('truncate table %s' % table_name)
#             curs.prepare(prepared_statement)
#             print(rows[0])
#             curs.executemany(None, rows)
#         con.commit()
