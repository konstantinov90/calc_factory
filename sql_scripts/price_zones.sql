select 1 price_zone_code, getfrsparameternumvalue('PConsYearEur')
from dual

union all

select 2 price_zone_code, getfrsparameternumvalue('PConsYearSib')
from dual
