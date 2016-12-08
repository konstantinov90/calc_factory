from sqlalchemy import *

metadata = MetaData()

nodes = Table('nodes', metadata,
            Column('node_code', Integer, primary_key=True),
            Column('area', Integer, nullable=True),
            Column('nominal_voltage', Numeric),
            Column('price_zone', Integer),
            Column('min_voltage', Numeric),
            Column('max_voltage', Numeric),
            Column('voltage_class', Numeric)
        )

lines = Table('lines', metadata,
            Column('node_from_code', Integer, ForeignKey('nodes.node_code'), primary_key=True),
            Column('node_to_code', Integer, ForeignKey('nodes.node_code'), primary_key=True),
            Column('parallel_num', Integer, primary_key=True),
            Column('kt_re', Numeric),
            Column('kt_im', Numeric),
            Column('div', Numeric),
            Column('type', Integer),
            Column('area', Integer, nullable=True)
        )
