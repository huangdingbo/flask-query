from CommonExpressBuilder import CommonExpressBuilder
from Handler import Handler
from LikeCondition import LikeCondition


class LikeConditionBuilder(CommonExpressBuilder):

    def build(self, expression: LikeCondition, params={}):
        operator = Handler.strToUpper(expression.getOperator())
        column = expression.getColumn()
        values = expression.getValue()
        escape = expression.escapingReplacements()

        if not values:
            return "0=1"

        self.addParams(values)

        return column + " " + operator + " " + "%s"
