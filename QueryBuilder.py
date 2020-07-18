from BetweenConditionBuilder import BetweenConditionBuilder
from ConjunctionConditionBuilder import ConjunctionConditionBuilder
from ExpressionInterface import ExpressionInterface
from HashCondition import HashCondition
from InConditionBuilder import InConditionBuilder
from LikeConditionBuilder import LikeConditionBuilder
from NotCondition import NotCondition
from AndCondition import AndCondition
from OrCondition import OrCondition
from BetweenCondition import BetweenCondition
from InCondition import InCondition
from LikeCondition import LikeCondition
from SimpleCondition import SimpleCondition
from Handler import Handler
from HashConditionBuilder import HashConditionBuilder
from NotConditionBuilder import NotConditionBuilder
from SimpleConditionBuilder import SimpleConditionBuilder


class QueryBuilder(object):
    _conditionClasses = {}

    _conditionBuilders = {}

    params = []

    def _defaultConditionClasses(self):
        return {
            "NOT": NotCondition,
            "AND": AndCondition,
            "OR": OrCondition,
            "BETWEEN": BetweenCondition,
            "NOT BETWEEN": BetweenCondition,
            "IN": InCondition,
            "NOT IN": InCondition,
            "LIKE": LikeCondition,
            "NOT LIKE": LikeCondition,
            "OR LIKE": LikeCondition,
            "OR NOT LIKE": LikeCondition,
        }

    def _defaultConditionBuilders(self):
        return {
            str(AndCondition): ConjunctionConditionBuilder,
            str(OrCondition): ConjunctionConditionBuilder,
            str(NotCondition): NotConditionBuilder,
            str(HashCondition): HashConditionBuilder,
            str(BetweenCondition): BetweenConditionBuilder,
            str(InCondition): InConditionBuilder,
            str(LikeCondition): LikeConditionBuilder,
            str(SimpleCondition): SimpleConditionBuilder,
        }

    def __init__(self):
        self._conditionClasses = self._defaultConditionClasses()
        self._conditionBuilders = self._defaultConditionBuilders()
        self.params = []

    def build(self, query):
        clauses = [
            self.buildSelect(query.select_prop, query.params, query.distinct_prop, query.selectOption_prop),
            self.buildTable(query.table_prop),
            self.buildJoin(query.join_prop, query.params),
            self.buildWhere(query.where_prop, query.params),
            self.buildGroupBy(query.groupBy_prop),
            self.buildHaving(query.having_prop, query.params),
            self.buildOrderBy(query.orderBy_prop),
            self.buildLimit(query.offset_prop, query.limit_prop)
        ]

        return " ".join(clauses), self.params

    def buildParams(self, columns, params):
        if not columns:
            return "*"
        for index, value in enumerate(columns):
            val = params.get(value, None)
            if val:
                columns[index] = val
        return columns

    def buildSelect(self, columns: list, params, distinct, selectOption):
        columns = self.buildParams(columns, params)

        select = "SELECT " + selectOption if selectOption else "SELECT "

        if distinct:
            index = columns.index(distinct)
            del columns[index]
            select += "DISTINCT " + distinct + ","

        return select + ",".join(columns)

    def buildTable(self, table: list):
        return "FROM " + ",".join(table)

    def buildJoin(self, columns, params):
        # join = self.buildParams(join, params)
        if columns:
            join = ""
            for item in columns:
                if not item[1] or not item[2]:
                    raise Exception("语法错误{}".format(str(item)))
                join += item[0] + " " + item[1] + " ON " + item[2]
            return join
        return ''

    def buildWhere(self, condition, params):

        where = self.buildCondition(condition, params)

        return '' if not where else "WHERE " + where

    def buildCondition(self, condition, params):
        if isinstance(condition, list) or isinstance(condition, dict):
            if not condition:
                return ""

            condition = self.createConditionFromArray(condition)

        if isinstance(condition, ExpressionInterface):
            return self.buildExpression(condition, params)

        return str(condition)

    def createConditionFromArray(self, condition):
        if isinstance(condition, list) and Handler.list_get(condition, 0, None):
            operator = Handler.strToUpper(condition.pop(0))
            if self._conditionClasses.get(operator, None):
                className = self._conditionClasses.get(operator)
            else:
                className = SimpleCondition

            return className.fromArrayDefinition(operator, condition)

        # 字典类型 {"key": "value"}
        return HashCondition(condition)

    def buildExpression(self, expression: ExpressionInterface, params={}):
        # 获取建造者
        builder = self.getExpressionBuilder(expression)

        return builder(self).build(expression, params)

    def getExpressionBuilder(self, expression: ExpressionInterface):
        builder = self._conditionBuilders.get(str(expression.__class__), None)

        if not builder:
            raise Exception("未匹配相应的建造者{}".format(str(expression.__class__)))

        return builder

    def buildGroupBy(self, columns):
        if not columns:
            return ""

        if isinstance(columns, str):
            return 'GROUP BY ' + columns

        if isinstance(columns, list):
            return "GROUP BY " + ",".join(columns)

        raise Exception("GROUP BY 只接受字符串或者列表,传递的类型是{}".format(str(type(columns))))

    def buildHaving(self, condition, params):
        having = self.buildCondition(condition, params)

        return '' if not having else "HAVING " + having

    def buildOrderBy(self, columns):
        if not columns:
            return ""

        if isinstance(columns, str):
            return "ORDER BY " + columns

        if isinstance(columns, list):
            return "ORDER BY " + ",".join(columns)

        raise Exception("ORDER BY 只接受字符串或者列表,传递的类型是{}".format(str(type(columns))))

    def buildLimit(self, offset, limit):
        if not limit:
            return ""

        limit = int(limit)
        offset = int(offset)

        return "LIMIT " + str(offset) + "," + str(limit)

    def buildInsert(self, table, datas: list):
        key_list = dict(datas[0]).keys()
        key_str = str(tuple(key_list)).replace("'", "")
        length = len(key_list)
        parts = []
        values = []
        for item in datas:
            values = values + list(item.values())
            place_str = str(('%s',) * length).replace("'", "")
            parts.append(place_str)
        sql = "INSERT INTO " + table + key_str + " VALUES" + ",".join(parts)
        return sql, values
