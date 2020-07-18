from CommonExpressBuilder import CommonExpressBuilder
from ExpressionInterface import ExpressionInterface


class ConjunctionConditionBuilder(CommonExpressBuilder):
    """
    结合条件建造者
    """

    def build(self, expression: ExpressionInterface, params={}):

        parts = self.buildExpressionsFrom(expression, params)

        if not parts:
            return ""

        return "(" + (") " + expression.getOperator() + " (").join(parts) + ")"

    def buildExpressionsFrom(self, condition: ExpressionInterface, params={}):
        parts = []

        for express in condition.getExpressions():
            if isinstance(express, list) or isinstance(express, dict):
                express = self._queryBuilder.buildCondition(express, params)

            if isinstance(express, ExpressionInterface):
                express = self._queryBuilder.buildExpression(express, params)

            if express:
                parts.append(express)

        return parts
