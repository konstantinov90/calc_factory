from eq_db.classes.stations import make_stations
from eq_db.vertica_corrections import add_stations_vertica


tsid = 220482901
scenario = 1
tdate = '31-07-2015'

stations = make_stations(tsid, tdate)

stations = add_stations_vertica(stations, scenario, tdate)
