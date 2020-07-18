from flask import current_app
from MysqlDB import MysqlDB
from DBUtils.PooledDB import PooledDB
from PoolDB import PoolDB


class DB(object):
    _dbConfig = {}

    def __init__(self):
        self._dbConfig = current_app.config['FLASK_QUERY_CONFIG']

    def getConnect(self):
        # pymysql
        if isinstance(self._dbConfig, dict):
            db, cursor = MysqlDB(self._dbConfig).getConnect()
            return db, cursor
        # DBUtils连接池
        elif isinstance(self._dbConfig, PooledDB):
            db, cursor = PoolDB(self._dbConfig).getConnect()
            return db, cursor

    def _get_db_uri(self):
        return self._dbConfig['engine'] + "+" + self._dbConfig['driver'] + "://" + self._dbConfig['user'] + ":" + \
               self._dbConfig[
                   'password'] + "@" + \
               self._dbConfig['host'] + ":" + self._dbConfig['port'] + "/" + self._dbConfig['database']
