import pymysql


class MysqlDB(object):
    _db = None

    _cursor = None

    def __init__(self, config: dict):
        self._db = pymysql.connect(host=config['host'], port=int(config['port']), db=config['database'],
                                   user=config['user'],
                                   passwd=config['password'], charset=config['charset'])
        self._cursor = self._db.cursor(cursor=pymysql.cursors.DictCursor)

    def getConnect(self):
        return self._db, self._cursor
