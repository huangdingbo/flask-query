from ConditionInterface import ConditionInterface
from abc import ABCMeta, abstractmethod


class ConjunctionCondition(ConditionInterface, metaclass=ABCMeta):
    """
    结合表达式的公共抽象类，AND 和 OR
    """
    _expressions = None

    def __init__(self, expressions):
        self._expressions = expressions

    def getExpressions(self):
        return self._expressions

    @abstractmethod
    def getOperator(self): pass

    @classmethod
    def fromArrayDefinition(cls, operator, operands):
        return cls(operands)


if __name__ == '__main__':
    print(ConjunctionCondition(1))
