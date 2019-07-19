#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import logging
import os
import configparser
from pandas import DataFrame
from datetime import datetime
from MySQLManager import MySQLInstance

start_time = datetime.now()

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
log_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'log_task_user_1907.log')
f_handler = logging.FileHandler(log_file)
f_handler.setLevel(logging.INFO)
formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
f_handler.setFormatter(formatter)
logger.addHandler(f_handler)
s_handler = logging.StreamHandler()
s_handler.setLevel(logging.DEBUG)
logger.addHandler(s_handler)

conf = configparser.ConfigParser()
conf_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config_task_user_1907.ini')
conf.read(conf_file, encoding='utf-8')

mysql_db_ppzck_task = {
    'host': conf.get('ppzck_task', 'host'),
    'port': conf.getint('ppzck_task', 'port'),
    'username': conf.get('ppzck_task', 'username'),
    'password': conf.get('ppzck_task', 'password'),
    'schema': conf.get('ppzck_task', 'schema')
}

mysql_db_bi_task = {
    'host': conf.get('bi_task', 'host'),
    'port': conf.getint('bi_task', 'port'),
    'username': conf.get('bi_task', 'username'),
    'password': conf.get('bi_task', 'password'),
    'schema': conf.get('bi_task', 'schema')
}

company_id = conf.get('account', 'email')
ss_time = conf.get('account', 'time')
order = conf.get('account', 'order').split()


def query_data_frame(db_dict, sql, result=True):
    with MySQLInstance(**db_dict, dict_result=result) as db:
        if db.query(sql):
            return DataFrame(db.query(sql))
        else:
            logger.info('No result.')
            sys.exit()


sql_1 = """SELECT Id from t_enterpriseuser WHERE email = '%s'"""

with MySQLInstance(**mysql_db_ppzck_task, dict_result=False) as db1:
    a = db1.query(sql_1 % company_id)

sql_2 = """SELECT id from t_task WHERE owner_id = '%s' AND create_time > '%s'"""

with MySQLInstance(**mysql_db_ppzck_task, dict_result=False) as db1:
    b = db1.query(sql_2 % (a[0][0], ss_time))
c = r"','".join([i[0] for i in b])

sql_3 = """SELECT tui.*, tu.phone, tu.pid_qq, tu.email, tu.realname, tu.zfbname, tu.nickname, tu.address 
from t_userinfo tui LEFT JOIN t_user tu ON tui.Id = tu.Id 
WHERE tu.id 
in (SELECT uid FROM t_response tr WHERE taskid_owner IN ('%s') GROUP BY uid)"""

d = query_data_frame(mysql_db_ppzck_task, sql_3 % c)
d.set_index('Id', inplace=True)

sql_4 = """SELECT a.uid, a.mark, count(0) FROM 
(SELECT uid, CASE WHEN `status` = '2' THEN 'B' WHEN `status` = '5' THEN 'B' ELSE 'A' END  mark FROM t_response tr 
WHERE taskid_owner IN ('%s')) a
GROUP BY a.uid, a.mark"""

e = query_data_frame(mysql_db_ppzck_task, sql_4 % c)
e = e.set_index(['uid', 'mark']).unstack()
e.columns = e.columns.droplevel(0)
e.fillna(0, inplace=True)
e['C'] = e['A'] + e['B']
f = d.join(e)
f.reset_index(inplace=True)
f.to_csv(company_id + '.csv', columns=order, index=False, encoding='utf_8_sig')
end_time = datetime.now()
logger.info('time_consumed: %s' % (end_time-start_time))
