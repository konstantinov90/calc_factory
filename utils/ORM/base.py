from sqlalchemy import *
from sqlalchemy.orm import reconstructor
from sqlalchemy.ext.declarative import as_declarative


@as_declarative()
class Base(object):
    # id = Column(Integer, primary_key=True, unique=True)
    # lst = {}
    pass

    # @reconstructor
    # def _init_on_load(self):
    #     if self.id not in self.lst.keys():
    #         self.lst[self.id] = self
        # print('hello from reconstructor!')
