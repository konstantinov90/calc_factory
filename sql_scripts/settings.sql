select set_code code,
set_type valueType,
nvl(string_value, '') string_value,
nvl(number_value, -1) number_value,
nvl(date_value - to_date('01.01.0001', 'DD-MM-YYYY') + 365, -1) tdate
from tsdb2.wh_eq_db_settings partition (&tsid)
order by upper(set_code)
