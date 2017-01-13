"""module to compare sequence and db table"""
from decimal import Decimal
from utils import DB
from utils.printer import print
from utils.zip_join import zip_join
from utils.progress_bar import update_progress

def csv_comparator(data, script, filename, *join_clause):
    """comparator function"""
    data_o = DB.OracleConnection().script_cursor(script)
    print('filling %s' % filename)
    if data:
        first_row = data[0]
    else:
        raise Exception('empty data!')

    frmt = ''
    for datum in first_row:
        if isinstance(datum, int):
            frmt += '%i;'
        elif isinstance(datum, (float, Decimal)):
            frmt += '%f;'
        else:
            raise Exception('wrong type %r' % first_row)

    with open(filename, 'w', encoding='utf-8') as _file:
        spoiled = False
        for i, (new_row, old_row) in enumerate(zip_join(data, data_o, *join_clause)):
            tup = tuple(Decimal(x) - Decimal(y) for x, y in zip(new_row, old_row))

            if sum(abs(i) for i in tup) > 1e-3:
                _file.write(frmt*3 % (tup + new_row + old_row) + '\n')
                if not spoiled:
                    print('------- check spoiled! -------')
                    spoiled = True
            else:
                _file.write(frmt % tup  + '\n')
            update_progress((i + 1) / len(data))
