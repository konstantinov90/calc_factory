import os
from itertools import cycle
from datetime import datetime
from numbers import Real
from collections import Sequence, Mapping
# from subprocess import call

def make_report_file(template, worksheet, input_data, out_file):
    """generate report from template"""
    dir_path = os.path.dirname(os.path.realpath(__file__))

    file_path = os.path.join(dir_path, 'make_report_template.vbs')
    with open(file_path, encoding='utf-8') as fs:
        vbs_tmp = fs.read()

    tmp_path = os.path.join(dir_path, 'tmp.vbs')
    with open(tmp_path, 'w') as fs:
        fs.write(vbs_tmp.format(
            template_name=template,
            worksheet=worksheet,
            data=input_data,
            report_name=out_file
            ))
    os.system(tmp_path)
    os.remove(tmp_path)

def generate_report(ora_con, script, template, worksheets, starts, out_file, kwargss):
    """get data and generate report"""
    print('making %s' % out_file)
    if not isinstance(worksheets, Sequence) or isinstance(worksheets, str):
        worksheets = [worksheets]
    if not isinstance(kwargss, Sequence) or isinstance(kwargss, Mapping):
        kwargss = [kwargss]
    if not isinstance(starts[0], Sequence):
        starts = [starts]

    if not len(worksheets) == len(starts) == len(kwargss):
        raise Exception('data errors!')

    N = len(worksheets)

    dir_path = os.path.dirname(os.path.realpath(__file__))
    template_path = os.path.join(dir_path, 'templates', template)
    tmp_file = os.path.join(dir_path, template)
    out_files = []
    for i in range(N):
        if i % 2:
            out_files.append(tmp_file)
        else:
            out_files.append(out_file)
    out_files.reverse()
    templates = [template_path] + out_files[:-1]

    for worksheet, start, kwargs, template, out_file in \
        zip(worksheets, starts, kwargss, templates, out_files):
        data = ''

        idx = list(start)
        for row in ora_con.script_cursor(script, **kwargs):
            for item in row:
                if item is not None:
                    if isinstance(item, datetime):
                        value = '"%s"' % item.strftime('%d.%m.%Y')
                    elif isinstance(item, str):
                        value = '"%s"' % item.replace('"', '""')
                    elif isinstance(item, Real):
                        value = '%r' % item
                    else:
                        raise Exception('unidentified value %r' % item)
                    data += 'cells(%i, %i).Value = %s\n' % (idx[0], idx[1], value)
                idx[1] += 1
            idx = [idx[0] + 1, start[1]]

        try:
            os.remove(out_file)
        except:
            pass
        make_report_file(template, worksheet, data, out_file)
    try:
        os.remove(tmp_file)
    except:
        pass
