from utils import ORM

from eq_db.classes.dpgs import make_dpgs
from eq_db.classes.dpgs.dpg_supply import DpgSupply

tsid = 220482901
scenario = 1
tdate = '31-07-2015'

ORM.recreate_all()

dpgs = make_dpgs(tsid, tdate)
