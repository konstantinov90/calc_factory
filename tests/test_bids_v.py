from eq_db.classes.bids import make_bids
from eq_db.vertica_corrections import add_bids_vertica

tsid = 220482901
scenario = 1
tdate = '31-07-2015'

bids = make_bids(tsid, tdate)
bids = add_bids_vertica(bids, scenario, tdate)
