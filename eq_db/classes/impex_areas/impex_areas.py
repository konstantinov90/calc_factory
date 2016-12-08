from sqlalchemy import *
from sqlalchemy.orm import reconstructor

from utils.ORM import Base, MetaBase
from sql_scripts import impex_area_script as ias
from settings import section_nkaz


class ImpexArea(Base, metaclass=MetaBase):
    __tablename__ = 'impex_areas'
    id = Column(Integer, primary_key=True)
    section_code = Column(Integer)
    area_code = Column(Integer)
    is_europe = Column(Boolean)
    is_optimizable = Column(Boolean)

    def __init__(self, ias_row):
        self.section_code = ias_row[ias['section_number']]
        self.area_code = ias_row[ias['area']]
        self.is_europe = True if ias_row[ias['is_europe']] else False
        self.is_optimizable = True if ias_row[ias['optimized']] else False

    @reconstructor
    def _init_on_load(self):
        if self.id not in self.lst.keys():
            self.lst[self.id] = self

    def serialize(self, session):
        session.add(self)
        session.flush()

    def get_impex_area_data(self):
        if self.section_code == section_nkaz:
            price_zone = 0
        elif self.is_europe:
            price_zone = 1
        else:
            price_zone = 2
        return ((self.section_code, self.area_code, price_zone))
