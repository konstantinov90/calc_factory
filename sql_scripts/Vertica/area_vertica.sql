select hour, area, losses, load_losses
from dm_opr.model_area_ts
where scenario_fk = :scenario
