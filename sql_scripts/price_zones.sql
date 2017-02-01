select 1 price_zone_code, to_number(param_value)
from tsdb2.wh_frs_parameter partition (&tsid)
where param_code = 'PConsYearEur'

union all

select 2 price_zone_code, to_number(param_value)
from tsdb2.wh_frs_parameter partition (&tsid)
where param_code = 'PConsYearSib'
