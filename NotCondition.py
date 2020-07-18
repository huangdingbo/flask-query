from ConditionInterface import ConditionInterface


class NotCondition(ConditionInterface):
    """
    NOT 条件
    """
    __condition = None

    def __init__(self, condition):
        self.__condition = condition

    def getCondition(self):
        return self.__condition

    @classmethod
    def fromArrayDefinition(cls, operator, operands):
        if len(operands) != 1:
            raise Exception("NOT 条件只需要一个操作数")

        return cls(operands)
