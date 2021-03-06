from .meta_base import MetaBase
from .base import Base
from .caching_query import FromCache

from sqlalchemy import create_engine
from sqlalchemy.engine import reflection
from sqlalchemy.schema import (
        MetaData,
        Table,
        DropTable,
        ForeignKeyConstraint,
        DropConstraint,
        )
from sqlalchemy.orm import sessionmaker
# from sqlalchemy.ext.declarative import declarative_base

from settings import postgre_connection_string, postgre_echo

engine = create_engine(postgre_connection_string, echo=postgre_echo)

session = sessionmaker(bind=engine)()

# Base = declarative_base()

def recreate_all():
    # Base.metadata.drop_all(engine)
    db_DropEverything(engine)
    Base.metadata.create_all(engine)

def db_DropEverything(engine):
    # From http://www.sqlalchemy.org/trac/wiki/UsageRecipes/DropEverything

    conn = engine.connect()

    # the transaction only applies if the DB supports
    # transactional DDL, i.e. Postgresql, MS SQL Server
    trans = conn.begin()

    inspector = reflection.Inspector.from_engine(engine)

    # gather all data first before dropping anything.
    # some DBs lock after things have been dropped in
    # a transaction.
    metadata = MetaData()

    tbs = []
    all_fks = []

    for table_name in inspector.get_table_names():
        fks = []
        for fk in inspector.get_foreign_keys(table_name):
            if not fk['name']:
                continue
            fks.append(
                ForeignKeyConstraint((),(),name=fk['name'])
                )
        t = Table(table_name,metadata,*fks)
        tbs.append(t)
        all_fks.extend(fks)

    for fkc in all_fks:
        conn.execute(DropConstraint(fkc))

    for table in tbs:
        conn.execute(DropTable(table))

    trans.commit()
