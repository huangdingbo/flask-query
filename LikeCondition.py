from SimpleCondition import SimpleCondition


class LikeCondition(SimpleCondition):
    """
    LIKE 和 NOT LIKE 条件
    """

    # 转译的字符
    _escapingReplacements = None

    @property
    def escapingReplacements(self):
        return self._escapingReplacements

    @escapingReplacements.setter
    def escapingReplacements(self, escapingReplacements):
        self._escapingReplacements = escapingReplacements

    def __init__(self, column, operator, value):
        super().__init__(column, operator, value)

    @classmethod
    def fromArrayDefinition(cls, operator, operands):
        if len(operands) < 2:
            raise Exception("LIKE 至少需要两个操作符")

        condition = cls(operands[0], operator, operands[1])

        if len(operands) == 3:
            condition.escapingReplacements = operands[2]

        return condition
