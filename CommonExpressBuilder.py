from abc import ABCMeta, abstractmethod
from ExpressionInterface import ExpressionInterface


class CommonExpressBuilder(metaclass=ABCMeta):
    """
    所有builder的基类
    """

    _queryBuilder = None

    def __init__(self, queryBuilder):
        self._queryBuilder = queryBuilder

    @abstractmethod
    def build(self, expression: ExpressionInterface, params={}): pass

    def addParams(self, value):
        self._queryBuilder.params.append(value)
