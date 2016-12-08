from sqlalchemy import *
from eq_db.schemas import Schemas

db_conn_string = 'postgresql://localhost/calc_factory'

engine = create_engine(db_conn_string, echo=True)

Schemas.metadata.drop_all(engine)

Schemas.metadata.create_all(engine)
