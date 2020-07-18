from ConjunctionCondition import ConjunctionCondition


class AndCondition(ConjunctionCondition):
    """
    AND 连接对象
    """
    def getOperator(self):
        return "AND"


if __name__ == '__main__':
    obj = AndCondition.fromArrayDefinition("aa", "bb")
    print(obj.getExpressions())
