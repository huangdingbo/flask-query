from CommonExpressBuilder import CommonExpressBuilder
from NotCondition import NotCondition


class NotConditionBuilder(CommonExpressBuilder):

    def build(self, expression: NotCondition, params={}):
        operand = expression.getCondition()
        if operand == "":
            return ""

        expression = self._queryBuilder.buildCondition(operand, params)
        return self._getNegationOperator() + "(" + expression + ")"

    def _getNegationOperator(self):
        return "NOT"
