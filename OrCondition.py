from ConjunctionCondition import ConjunctionCondition


class OrCondition(ConjunctionCondition):
    """
    OR 连接对象
    """
    def getOperator(self):
        return "OR"


if __name__ == '__main__':
    obj = OrCondition.fromArrayDefinition('cc', 'dd')
    print(obj.getExpressions())
