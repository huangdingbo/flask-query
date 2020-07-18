class BaseQuery(object):
    params = {}

    def _formatCondition(self, condition):
        """
        格式化where条件
        :param condition:
        :return:
        """
        if not condition:
            raise Exception("条件不能为空")
        elif isinstance(condition, str):
            return condition
        elif isinstance(condition, list):
            return condition
        elif isinstance(condition, dict):
            res = []
            for key, value in condition.items():
                res.append(["=", key, value])
            return res

        raise Exception("不支持的参数类型{}".format(str(condition)))

    def _list_get(self, L, i, v=None):
        if -len(L) <= i <= len(L):
            return L[i]
        else:
            return v

    def _filterWhere(self, condition):
        if not condition or isinstance(condition, str):
            return False

        if isinstance(condition, dict):
            condition = self._filterDict(condition)
            if not condition:
                return False

        if isinstance(condition, list):
            if not self._filterList(condition):
                return False

        return condition

    def _filterDict(self, condition: dict):
        result = {}
        for key, value in condition.items():
            if not self._isEmpty(value):
                result[key] = value
        return result

    def _filterList(self, condition: list):
        operation = self._formatOperator(condition[0])
        if operation == "BETWEEN" or operation == "NOT BETWEEN":
            if self._isEmpty(self._list_get(condition, 2)) or self._isEmpty(self._list_get(condition, 3)):
                return False
        else:
            if self._isEmpty(self._list_get(condition, 2)):
                return False

        return condition

    def _isEmpty(self, value):
        if value == {} or value == [] or value == () or value == '':
            return True
        return False

    def _dealOperatorWhere(self, name, value, operator="="):
        operator = self._formatOperator(operator)
        if operator == "=":
            where = {name: value}
        elif operator in [">", ">=", "<", "<=", "<>", "!=", "IN", "NOT IN"]:
            where = [operator, name, value]
        else:
            raise Exception("不支持的操作符{}".format(str(operator)))
        return where

    def _formatOperator(self, operator):

        return str(operator).strip().upper()

    def _formatColumns(self, columns):
        if isinstance(columns, list):
            result = list(tuple(columns))
        elif isinstance(columns, tuple):
            result = list(columns)
        elif isinstance(columns, str):
            result = list(tuple(columns.split(",")))
        else:
            raise Exception("不支持的类型{}".format(str(type(columns))))
        return result

    def print_where(self):
        print(str(self.where_prop))

    def getUpdateConditionParams(self, condition):
        params = []
        if isinstance(condition, dict):
            for key, value in condition.items():
                params.append(value)

        if isinstance(condition, list):
            for item in condition:
                if isinstance(item, dict):
                    for key, value in item.items():
                        params.append(value)
                if isinstance(item, list):
                    params.append(item[2])

        return params
