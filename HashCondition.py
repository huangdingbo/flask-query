from ConditionInterface import ConditionInterface


class HashCondition(ConditionInterface):
    """
    hash == {"key":"value"}
    """
    __hash = None

    def __init__(self, hash):
        self.__hash = hash

    def getHash(self):
        return self.__hash

    @classmethod
    def fromArrayDefinition(cls, operator, operands):
        return cls(operands)
