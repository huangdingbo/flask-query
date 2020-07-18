from ExpressionInterface import ExpressionInterface
from abc import ABCMeta, abstractmethod


class ConditionInterface(ExpressionInterface, metaclass=ABCMeta):
    """
    所有条件对象的接口
    """

    @classmethod
    @abstractmethod
    def fromArrayDefinition(cls, operator, operands):
        """
        通过列表定义创建表达式对象
        :param operator:运算符，大写
        :param operands:对应操作的列表
        :return: self 返回当前类的实例
        """
        pass
