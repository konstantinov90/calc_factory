select f.fuel_type_fk, f.factor
from dm_opr.model_factor_fuel_price f, dm_opr.model_scenarios s
where s.scenario_pk = :scenario
and f.year = to_number(to_char(s.date_model,'yyyy'))
and f.base_year = to_number(to_char(s.date_ts,'yyyy'))
