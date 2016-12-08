import time
from utils import DB, ORM
from utils.progress_bar import update_progress
from sql_scripts import rastr_consumer_script as rc
from .consumers import Consumer


def make_consumers(tsid={}, tdate=''):
    if isinstance(tsid, int):
        tsid = {'tsid': tsid}
    print('making consumers%s' % ((' for date %s' % tdate) if tdate else ''))
    start_time = time.time()

    con = DB.OracleConnection()

    consumers = ConsumersList()

    @DB.process_cursor(con, rc, tsid)
    def process_consumers(new_row, consumers_list):
        code = new_row[rc['consumer_code']]
        consumer = consumers_list[code]
        if not consumer:
            consumers_list.add_consumer(new_row)
        consumers_list[code].add_consumer_hour_data(new_row)

    process_consumers(consumers)
    ORM.session.add_all(consumers.consumers_list)
    for i, consumer in enumerate(consumers):
        ORM.session.add_all(consumer.consumer_hour_data.values())
        update_progress((i + 1) / len(consumers))
    ORM.session.commit()


    print('%s %s seconds %s' % (15 * '-', round(time.time() - start_time, 3), 15 * '-'))

    return consumers


class ConsumersList(object):
    def __init__(self):
        self.consumers_list = []
        self.consumers_list_index = {}

    def __len__(self):
        return len(self.consumers_list)

    def __iter__(self):
        for c in self.consumers_list:
            yield c

    def __getitem__(self, item):
        if item in self.consumers_list_index.keys():
            return self.consumers_list[self.consumers_list_index[item]]
        else:
            return None

    def add_consumer(self, rc_row):
        consumer_code = rc_row[rc['consumer_code']]
        self.consumers_list_index[consumer_code] = len(self.consumers_list)
        self.consumers_list.append(Consumer(rc_row))
