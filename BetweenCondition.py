from ConditionInterface import ConditionInterface


class BetweenCondition(ConditionInterface):
    """
    BETWEEN (expression1) AND (expression2) 条件
    """
    __operator = None

    __column = None

    __intervalStart = None

    __intervalEnd = None

    def __init__(self, column, operator, intervalStart, intervalEnd):
        """
        :param column: operator 左边的文字
        :param operator::"BETWEEN"或"NOT BETWEEN"
        :param intervalStart:开始区间
        :param intervalEnd:结束区间
        """
        self.__column = column
        self.__operator = operator
        self.__intervalStart = intervalStart
        self.__intervalEnd = intervalEnd

    def getOperator(self):
        return self.__operator

    def getColumn(self):
        return self.__column

    def getIntervalStart(self):
        return self.__intervalStart

    def getIntervalEnd(self):
        return self.__intervalEnd

    @classmethod
    def fromArrayDefinition(cls, operator, operands):
        """
        :param operator: 操作符 BETWEEN
        :param operands: 0 => 列,1 => 开始区间,2 => 结束区间
        :return:
        """
        if len(operands) != 3:
            raise Exception("BETWEEN 操作需要三个操作数")

        return cls(operands[0], operator, operands[1], operands[2])
