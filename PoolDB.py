import pymysql


class PoolDB(object):
    _db = None

    _cursor = None

    def __init__(self, poolDB):
        self._db = poolDB.connection()
        self._cursor = self._db.cursor(cursor=pymysql.cursors.DictCursor)

    def getConnect(self):
        return self._db, self._cursor
