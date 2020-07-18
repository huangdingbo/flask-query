from BaseQuery import BaseQuery
from QueryBuilder import QueryBuilder
from DB import DB


class Query(BaseQuery):
    """
    通过Query对象构建sql语句，通过createCommand()传入数据库连接对象，或者在flask的FLASK-QUERY-CONFIG配置数据库连接,
    执行sql语句
    """
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
        """
        初始化Query类属性
        """
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
        """
        设置查询的数据表
        :param table: 设置数据表,接受字符串或者列表
        :return:: self
        """
        if isinstance(table, str):
            self.table_prop = table.split(',')
        else:
            self.table_prop = table

        return self

    def leftJoin(self, table: str, on: str):
        """
        设置左外连接
        :param table: 左外连接的数据表，字符串类型
        :param on: 连接条件,字符串
        :return:: self
        """
        self.join_prop.append(["LEFt JOIN", table, on])
        return self

    def rightJoin(self, table: str, on: str):
        """
        设置右外连接
        :param table: 右外连接的数据表，字符串类型
        :param on: 连接条件,字符串
        :return:: self
        """
        self.join_prop.append(["RIGHT JOIN", table, on])
        return self

    def innerJoin(self, table: str, on: str):
        """
        设置内连接
        :param table: 内连接的数据表，字符串类型
        :param on: 连接条件,字符串
        :return:: self
        """
        self.join_prop.append(["INNER JOIN", table, on])
        return self

    def join(self, joinType: str, table: str, on: str):
        """
        指定类型连接
        :params joinType: 连接类型
        :param table: 连接的数据表，字符串类型
        :param on: 连接条件,字符串
        :return:: self
        """
        joinType = self._formatOperator(joinType)
        if joinType.replace(" ", "") not in ["LEFtJOIN", "RIGHTJOIN", "INNERJOIN"]:
            raise Exception("不支持的连接类型{}".format(str(joinType)))
        self.join_prop.append([joinType, table, on])
        return self

    def select(self, columns, option=None):
        """
        设置查询字段
        :param columns: 查询的列，接受字符串或者列表或者元组
        :param option: select的限定条件，eg:NO_CACHE
        :return:: self
        """
        self.select_prop = self._formatColumns(columns)
        self.selectOption_prop = option
        return self

    def addSelect(self, columns):
        """
        增加查询字段
        :param columns:增加查询的列，接受字符串或者列表或者元组
        :return:: self
        """
        columns = self._formatColumns(columns)
        self.select_prop = list(tuple(self.select_prop + columns))
        return self

    def distinct(self, value: str):
        """
        设置去重字段
        :param value: 去重字段，字符串类型
        :return:: self
        """
        self.distinct_prop = value
        return self

    def where(self, condition):
        """
        设置查询条件
        :param condition: 查询条件,接受字符串或者字典或者列表类型
        :return:: self
        """
        self.where_prop = condition
        return self

    def andWhere(self, condition):
        """
        增加查询条件
        :param condition: 查询条件,接受字符串或者字典或者列表类型
        :return:: self
        """
        if not self.where_prop:
            self.where_prop = condition
        elif isinstance(self.where_prop, list) and self._list_get(self.where_prop, 0) == "and":
            self.where_prop.append(condition)
        else:
            self.where_prop = ["and", self.where_prop, condition]
        return self

    def andFilterWhere(self, condition):
        """
        如果过滤条件的值为真，则增加查询条件
        :param condition: 查询条件,接受字符串或者字典或者列表类型
        :return:: self
        """
        condition = self._filterWhere(condition)
        if condition:
            self.andWhere(condition)
        return self

    def andOperatorWhere(self, name, value, operator="="):
        """
        以关键字参数的形式增加查询条件
        :param name: 字段名
        :param value: 值
        :param operator: 操作符
        :return:: self
        """
        where = self._dealOperatorWhere(name, value, operator)
        self.andWhere(where)
        return self

    def andFilterOperatorWhere(self, name, value, operator="="):
        """
        以关键字参数的形式增加查询条件,如果值为真
        :param name: 字段名
        :param value: 值
        :param operator: 操作符
        :return:: self
        """
        where = self._dealOperatorWhere(name, value, operator)
        self.andFilterWhere(where)
        return self

    def orWhere(self, condition):
        """
        添加or查询条件
        :param condition: 查询条件,接受字符串或者字典或者列表类型
        :return:: self
        """
        if not self.where_prop:
            self.where_prop = condition
        else:
            self.where_prop = ["or", self.where_prop, condition]
        return self

    def orFilterWhere(self, condition):
        """
        添加or查询条件,如果条件值为真
        :param condition: 查询条件,接受字符串或者字典或者列表类型
        :return:: self
        """
        condition = self._filterWhere(condition)
        if condition:
            self.orWhere(condition)
        return self

    def groupBy(self, columns):
        """
        设置分组字段
        :param columns: 分组的列，接受字符串或者列表
        :return:: self
        """
        self.groupBy_prop = self._formatColumns(columns)
        return self

    def addGroupBy(self, columns):
        """
        增加分组字段
        :param columns: 分组的列，接受字符串或者列表
        :return:: self
        """
        columns = self._formatColumns(columns)
        self.groupBy_prop = list(tuple(self.groupBy_prop + columns))
        return self

    def having(self, condition):
        """
        设置分组后的条件过滤
        :param condition: 查询条件,接受字符串或者字典或者列表类型
        :return:: self
        """
        self.having_prop = condition
        return self

    def andHaving(self, condition):
        """
        增加分组后的条件过滤
        :param condition: 查询条件,接受字符串或者字典或者列表类型
        :return: self
        """
        if not self.having_prop:
            self.having_prop = condition
        elif isinstance(self.having_prop, list) and self._list_get(self.having_prop, 0) == "and":
            self.having_prop.append(condition)
        else:
            self.having_prop = ["and", self.having_prop, condition]
        return self

    def andFilterHaving(self, condition):
        """
        如果过滤条件的值为真，则增加过滤条件
        :param condition: 查询条件,接受字符串或者字典或者列表类型
        :return: self
        """
        condition = self._filterWhere(condition)
        if condition:
            self.andHaving(condition)
        return self

    def andOperatorHaving(self, name, value, operator="="):
        """
        以关键字参数的形式增加分组后的过滤条件
        :param name: 字段名
        :param value: 值
        :param operator: 操作符
        :return: self
        """
        having = self._dealOperatorWhere(name, value, operator)
        self.andHaving(having)
        return self

    def andFilterOperatorHaving(self, name, value, operator="="):
        """
        以关键字参数的形式增加分组后的过滤条件,如果值为真
        :param name: 字段名
        :param value: 值
        :param operator: 操作符
        :return: self
        """
        having = self._dealOperatorWhere(name, value, operator)
        self.andFilterHaving(having)
        return self

    def orHaving(self, condition):
        """
        添加or过滤条件
        :param condition: 查询条件,接受字符串或者字典或者列表类型
        :return: self
        """
        if not self.having_prop:
            self.having_prop = condition
        else:
            self.having_prop = ["or", self.having_prop, condition]
        return self

    def orFilterHaving(self, condition):
        """
        添加or过滤条件,如果条件值为真
        :param condition: 查询条件,接受字符串或者字典或者列表类型
        :return: self
        """
        condition = self._filterWhere(condition)
        if condition:
            self.orHaving(condition)
        return self

    def orderBy(self, columns):
        """
        设置排序字段
        :param columns: 排序的字段，接受字符串或者列表
        :return: self
        """
        self.orderBy_prop = self._formatColumns(columns)
        return self

    def addOrderBy(self, columns):
        """
        增加排序字段
        :param columns: 排序的字段，接受字符串或者列表
        :return: self
        """
        columns = self._formatColumns(columns)
        self.orderBy_prop = list(tuple(self.orderBy_prop + columns))
        return self

    def limit(self, value):
        """
        限定数据返回条数
        :param value: 条数，整型
        :return: self
        """
        self.limit_prop = int(value)
        return self

    def offset(self, value):
        """
        设置偏移量
        :param value: 条数，整型
        :return: self
        """
        self.offset_prop = int(value)
        return self

    def createCommand(self, db=None, cursor=None):
        """
        设置数据库连接对象和游标，如果为空则使用flask配置
        :param db: 数据库连接对象
        :param cursor:游标对象
        :return: self
        """
        if not db or not cursor:
            self._db, self._cursor = DB().getConnect()
        else:
            self._db = db
            self._cursor = cursor

        return self

    def getRowSql(self):
        """
        获取sql语句
        :return: sql和参数
        """
        if self._type == 1:
            self.builderSql()
        return self._sql, self._params

    def builderSql(self):
        """
        构建查询sql
        """
        if not self.table_prop:
            raise Exception("table是必须的参数")
        self._sql, self._params = QueryBuilder().build(self)

    def all(self):
        """
        获取所有结果集
        :return: 查询结果
        """
        try:
            self.builderSql()
            self.execute()
            return self._cursor.fetchall()
        except Exception as e:
            raise Exception(str(e))

    def _close(self):
        """
        正常关闭数据库连接，调用commit
        """
        self._db.commit()
        self._db.close()

    def _error_close(self):
        """
        出错时关闭连接，调用rollback
        """
        self._db.rollback()
        self._db.close()

    def one(self):
        """
        获取一条数据
        :return: 查询结果
        """
        try:
            self.builderSql()
            self.execute()
            return self._cursor.fetchone()
        except Exception as e:
            raise Exception(str(e))

    def insert(self, table, data: dict):
        """
        插入
        :param table: 执行的数据表
        :param data: 需要插入的数据
        :return: self
        """
        self._type = 2
        self._sql, self._params = QueryBuilder().buildInsert(table, [data])
        return self

    def bathInsert(self, table, datas: list):
        """
        批量插入
        :param table: 执行的数据表
        :param datas: 需要插入的数据列表
        :return: self
        """
        self._type = 2
        self._sql, self._params = QueryBuilder().buildInsert(table, datas)
        return self

    def update(self, table, data, where=None):
        """
        更新
        :param table: 执行的数据表
        :param data: 需要插入的数据
        :param where: 更新的条件
        :return: self
        """
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
        """
        删除
        :param table: 执行的数据表
        :param where: 删除的条件
        :return: self
        """
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
        """
        更新或者插入
        :param table: 执行的数据表
        :param data: 更新或者插入的数据
        :param ignore: 当数据存在时是否忽略，默认忽略
        :param primary_key: 主键id
        :return: self
        """
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
        """
        检查更新、删除的条件
        :param where: 条件
        :return: self
        """
        if type(where) not in [dict, list]:
            raise Exception("where条件只支持字典、列表,where类型为{}".format(str(type(where))))

    def execute(self):
        """
        执行sql语句
        :return: bool
        """
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
