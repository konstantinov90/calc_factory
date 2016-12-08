from sqlalchemy.orm import relationship

from utils import ORM
from eq_db.classes.consumers import make_consumers, Consumer

tsid = 220482901
scenario = 1
tdate = '31-07-2015'


ORM.recreate_all()

consumers = make_consumers(tsid, tdate)
