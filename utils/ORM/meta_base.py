from sqlalchemy.ext.declarative import DeclarativeMeta


class MetaBase(DeclarativeMeta):
    def __new__(mcls, name, bases, namespace):
        cls = super().__new__(mcls, name, bases, namespace)
        if not hasattr(cls, 'lst'):
            cls.lst = {}
            cls.ky = None
        else:
            cls.ky = list(cls.lst.keys())[0]
        return cls

    def __getitem__(cls, item):
        if isinstance(item, tuple) and len(item) == 2:
            return cls.lst[item[0]][item[1]]
        if item in cls.lst.keys():
            return cls.lst[item]
        return None

    def __iter__(cls):
        if cls.ky:
            _lst = cls.lst[cls.ky]
        else:
            _lst = cls.lst
        for item in _lst.values():
            yield item

    # def __len__(cls):
    #     if cls.ky:
    #         return len(cls.lst[cls.ky])
    #     else:
    #         return len(cls.lst)
