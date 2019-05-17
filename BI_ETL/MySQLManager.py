#!/usr/bin/env python3
# -- coding utf-8 --

import pymysql
import importlib
import sys
import logging
importlib.reload(sys)  # python3写法, python2写法：reload(sys) sys.setdefaultencoding('utf8')


class MySQLCursor:  # 创建一个游标类

    def __init__(self, cursor, logger):
        self.cursor = cursor
        self.logger = logger

    def execute(self, sql, params=None):
        self.logger.info(sql+str(params))
        self.cursor.execute(sql, params)
        self.cursor.execute('Select last_insert_id()')
        res = self.cursor.fetchone()
        if len(res) == 1:
            if isinstance(res, dict):
                return res['last_insert_id()']
            if isinstance(res, tuple):
                return res[0]
        return 0

    def query(self, sql, params=None, with_description=False):
        self.logger.info(sql+str(params))
        self.cursor.execute(sql, params)
        rows = self.cursor.fetchall()
        if with_description:
            res = rows, self.cursor.description
        else:
            res = rows
        return res


class MySQLInstance:  # 创建一个实例类

    def __init__(self, host, port, username, password, schema=None, charset='utf8', dict_result=False, logger=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.schema = schema
        self.charset = charset
        self.dict_result = dict_result
        if logger is None:
            logger = logging.getLogger(__name__)
        self.logger = logger

    def __enter__(self):
        self.con = pymysql.connect(host=self.host, port=self.port, user=self.username, password=self.password,
                                   charset=self.charset, db=self.schema)
        if self.dict_result:
            self.cursor = self.con.cursor(pymysql.cursors.DictCursor)
        else:
            self.cursor = self.con.cursor()
        return MySQLCursor(self.cursor, self.logger)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.execute('commit')
        self.cursor.close()
        self.con.close()



