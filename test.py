import datetime
from utils import DB

con = DB.VerticaConnection()

query = '''
select *
from dm_opr.MODEL_GU_TS where date = :tdate
'''

class A(object):
    def __init__(self):
        self.query = query

    def get_query(self):
        return self.query

@DB.process_cursor(con, A(), {'tdate': datetime.date(2017, 4, 16)})
def process_data(new_row, data):
    data.append(new_row)
    # print(new_row)

data = []
process_data(data)

[print(d) for d in data]
