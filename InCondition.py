from ConditionInterface import ConditionInterface


class InCondition(ConditionInterface):
    """
    IN 和 NOT In 条件
    """
    __operator = None

    __column = None

    __values = []

    def __init__(self, column, operator, values):
        self.__column = column
        self.__operator = operator
        self.__values = values

    def getOperator(self):
        return self.__operator

    def getColumn(self):
        return self.__column

    def getValues(self):
        return self.__values

    @classmethod
    def fromArrayDefinition(cls, operator, operands):
        """
        :param operator: 操作符 IN 和 NOT IN
        :param operands: 0 => 列名(column),1 => 值(values)
        :return:
        """
        if len(operands) != 2:
            raise Exception("IN 和 NOT IN 操作需要两个操作数")

        return cls(operands[0], operator, operands[1])
