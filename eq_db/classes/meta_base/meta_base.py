"""Abstract class MetaBase."""
import abc


class MetaBase(abc.ABCMeta):
    """Base abstract metaclass MetaBase"""
    def __new__(mcs, name, bases, namespace):
        cls = super().__new__(mcs, name, bases, namespace)
        if not hasattr(cls, 'lst'):
            cls.lst = {}
            cls.key = None
        else:
            if not hasattr(cls, 'key'):
                cls.key = list(cls.lst.keys())[0]
        return cls

    def __getitem__(cls, item):
        if isinstance(item, tuple) and len(item) == 2:
            if item[1] in cls.lst[item[0]].keys():
                return cls.lst[item[0]][item[1]]
        if item in cls.lst.keys():
            return cls.lst[item]
        return None

    def __iter__(cls):
        if cls.key:
            _lst = cls.lst[cls.key]
        else:
            _lst = cls.lst
        for item in _lst.values():
            yield item

    def __len__(cls):
        if cls.key:
            return len(cls.lst[cls.key])
        else:
            return len(cls.lst)

    # def clear(cls):
    #     if cls.key:
    #         for key in cls.lst.keys():
    #             cls.lst[key] = {}
    #     else:
    #         cls.lst = {}
