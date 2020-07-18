from CommonExpressBuilder import CommonExpressBuilder
from Handler import Handler
from InCondition import InCondition


class InConditionBuilder(CommonExpressBuilder):
    """
    IN 应该是要支持子查询的，现在暂时不做
    """

    def build(self, expression: InCondition, params={}):
        operator = Handler.strToUpper(expression.getOperator())
        column = expression.getColumn()
        values = expression.getValues()

        if not column:
            return "0=1" if operator == "IN" else ""

        for index, val in enumerate(values):
            values[index] = "%s"
            self.addParams(val)

        valuesStr = "(" + ",".join(values) + ")"

        return column + " " + operator + " " + valuesStr
