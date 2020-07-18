from BaseQuery import BaseQuery
from QueryBuilder import QueryBuilder
from DB import DB


class Query(BaseQuery):
    table_prop = None

    join_prop = []

    select_prop = []

    distinct_prop = []

    selectOption_prop = None

    where_prop = []

    groupBy_prop = None

    having_prop = []

    orderBy_prop = []

    offset_prop = 0

    limit_prop = None

    _sql = None

    _db = None

    _cursor = None

    _params = None

    # 类型 1 select 2 update\insert\delete
    _type = 1

    def __init__(self):
        self.table_prop = None
        self.join_prop = []
        self.select_prop = []
        self.distinct_prop = []
        self.selectOption_prop = None
        self.where_prop = []
        self.groupBy_prop = None
        self.having_prop = []
        self.orderBy_prop = []
        self.offset_prop = 0
        self.limit_prop = None

    def table(self, table):
        if isinstance(table, str):
            self.table_prop = table.split(',')
        else:
            self.table_prop = table

        return self

    def leftJoin(self, table: str, on: str):
        self.join_prop.append(["LEFt JOIN", table, on])
        return self

    def rightJoin(self, table: str, on: str):
        self.join_prop.append(["RIGHT JOIN", table, on])
        return self

    def innerJoin(self, table: str, on: str):
        self.join_prop.append(["INNER JOIN", table, on])
        return self

    def join(self, joinType: str, table: str, on: str):
        joinType = self._formatOperator(joinType)
        if joinType.replace(" ", "") not in ["LEFtJOIN", "RIGHTJOIN", "INNERJOIN"]:
            raise Exception("不支持的连接类型{}".format(str(joinType)))
        self.join_prop.append([joinType, table, on])
        return self

    def select(self, columns, option=None):
        self.select_prop = self._formatColumns(columns)
        self.selectOption_prop = option
        return self

    def addSelect(self, columns):
        columns = self._formatColumns(columns)
        self.select_prop = list(tuple(self.select_prop + columns))
        return self

    def distinct(self, value: str):
        self.distinct_prop = value
        return self

    def where(self, condition):
        self.where_prop = condition
        return self

    def andWhere(self, condition):
        if not self.where_prop:
            self.where_prop = condition
        elif isinstance(self.where_prop, list) and self._list_get(self.where_prop, 0) == "and":
            self.where_prop.append(condition)
        else:
            self.where_prop = ["and", self.where_prop, condition]
        return self

    def andFilterWhere(self, condition):
        condition = self._filterWhere(condition)
        if condition:
            self.andWhere(condition)
        return self

    def andOperatorWhere(self, name, value, operator="="):
        where = self._dealOperatorWhere(name, value, operator)
        self.andWhere(where)
        return self

    def andFilterOperatorWhere(self, name, value, operator="="):
        where = self._dealOperatorWhere(name, value, operator)
        self.andFilterWhere(where)
        return self

    def orWhere(self, condition):
        if not self.where_prop:
            self.where_prop = condition
        else:
            self.where_prop = ["or", self.where_prop, condition]
        return self

    def orFilterWhere(self, condition):
        condition = self._filterWhere(condition)
        if condition:
            self.orWhere(condition)
        return self

    def groupBy(self, columns):
        self.groupBy_prop = self._formatColumns(columns)
        return self

    def addGroupBy(self, columns):
        columns = self._formatColumns(columns)
        self.groupBy_prop = list(tuple(self.groupBy_prop + columns))
        return self

    def having(self, condition):
        self.having_prop = condition
        return self

    def andHaving(self, condition):
        if not self.having_prop:
            self.having_prop = condition
        elif isinstance(self.having_prop, list) and self._list_get(self.having_prop, 0) == "and":
            self.having_prop.append(condition)
        else:
            self.having_prop = ["and", self.having_prop, condition]
        return self

    def andFilterHaving(self, condition):
        condition = self._filterWhere(condition)
        if condition:
            self.andHaving(condition)
        return self

    def andOperatorHaving(self, name, value, operator="="):
        having = self._dealOperatorWhere(name, value, operator)
        self.andHaving(having)
        return self

    def andFilterOperatorHaving(self, name, value, operator="="):
        having = self._dealOperatorWhere(name, value, operator)
        self.andFilterHaving(having)
        return self

    def orHaving(self, condition):
        if not self.having_prop:
            self.having_prop = condition
        else:
            self.having_prop = ["or", self.having_prop, condition]
        return self

    def orFilterHaving(self, condition):
        condition = self._filterWhere(condition)
        if condition:
            self.orHaving(condition)
        return self

    def orderBy(self, columns):
        self.orderBy_prop = self._formatColumns(columns)
        return self

    def addOrderBy(self, columns):
        columns = self._formatColumns(columns)
        self.orderBy_prop = list(tuple(self.orderBy_prop + columns))
        return self

    def limit(self, value):
        self.limit_prop = int(value)
        return self

    def offset(self, value):
        self.offset_prop = int(value)
        return self

    def createCommand(self, db=None, cursor=None):
        if not db or not cursor:
            self._db, self._cursor = DB().getConnect()
        else:
            self._db = db
            self._cursor = cursor

        return self

    def getRowSql(self):
        if self._type == 1:
            self.builderSql()
        return self._sql, self._params

    def builderSql(self):
        if not self.table_prop:
            raise Exception("table是必须的参数")
        self._sql, self._params = QueryBuilder().build(self)

    def all(self):
        try:
            self.builderSql()
            self.execute()
            return self._cursor.fetchall()
        except Exception as e:
            raise Exception(str(e))

    def _close(self):
        self._db.commit()
        self._db.close()

    def _error_close(self):
        self._db.rollback()
        self._db.close()

    def one(self):
        try:
            self.builderSql()
            self.execute()
            return self._cursor.fetchone()
        except Exception as e:
            raise Exception(str(e))

    def insert(self, table, data: dict):
        self._type = 2
        self._sql, self._params = QueryBuilder().buildInsert(table, [data])
        return self

    def bathInsert(self, table, datas: list):
        self._type = 2
        self._sql, self._params = QueryBuilder().buildInsert(table, datas)
        return self

    def update(self, table, data, where=None):
        self._type = 2
        sql = "UPDATE " + table + " SET "
        last_key = list(data.keys())[-1]
        self._params = []
        for key, value in data.items():
            self._params.append(value)
            if key == last_key:
                sql += str(key) + "=" + "%s"
            else:
                sql += str(key) + "=" + "%s,"

        if where:
            self._checkWhere(where)
            builder = QueryBuilder()
            where = builder.buildWhere(where, {})
            builderParams = builder.params
            self._params = self._params + builderParams
            sql += " " + where

        self._sql = sql

        return self

    def delete(self, table, where=None):
        self._type = 2
        self._sql = "DELETE FROM " + table
        if where:
            self._checkWhere(where)
            builder = QueryBuilder()
            where = builder.buildWhere(where, {})
            builderParams = builder.params
            self._params = self._params + builderParams
            self._sql += " " + where
        return self

    def upsert(self, table, data: dict, ignore=True, primary_key="id"):
        self._type = 2
        condition = {primary_key: data[primary_key]}
        del data[primary_key]
        res = Query().table(table).select(primary_key).where(condition).one()
        if res:
            if ignore:
                return self
            else:
                self.update(table=table, data=data, where=condition)
        else:
            self.insert(table=table, data=data)

        return self

    def _checkWhere(self, where):
        if type(where) not in [dict, list]:
            raise Exception("where条件只支持字典、列表,where类型为{}".format(str(type(where))))

    def execute(self):
        try:
            if not self._db or not self._cursor:
                self.createCommand()
            if self._sql:
                self._cursor.execute(self._sql, self._params)
            self._close()
            return True
        except Exception as e:
            self._error_close()
            raise Exception(str(e))
