from ConditionInterface import ConditionInterface


class SimpleCondition(ConditionInterface):
    """
    简单条件，比如 = > >= < <= ......
    """
    __operator = None

    __column = None

    __value = None

    def __init__(self, column, operator, value):
        self.__column = column
        self.__operator = operator
        self.__value = value

    def getColumn(self):
        return self.__column

    def getOperator(self):
        return self.__operator

    def getValue(self):
        return self.__value

    @classmethod
    def fromArrayDefinition(cls, operator, operands):
        if len(operands) != 2:
            raise Exception("simple 条件应该包含两个操作数")

        return cls(operands[0], operator, operands[1])
