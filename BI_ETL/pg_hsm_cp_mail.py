#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import logging
import os
import configparser
import pandas as pd
from pandas import DataFrame
from datetime import datetime
from sqlalchemy import create_engine
from MySQLManager import MySQLInstance
from EmailSender import EmailSender

start_time = datetime.now()

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
log_file = os.path.join(os.path.dirname(os.path.realpath('__file__')), 'log.log')
f_handler = logging.FileHandler(log_file)
f_handler.setLevel(logging.INFO)
formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
f_handler.setFormatter(formatter)
logger.addHandler(f_handler)
s_handler = logging.StreamHandler()
s_handler.setLevel(logging.DEBUG)
logger.addHandler(s_handler)

conf = configparser.ConfigParser()
conf_file = os.path.join(os.path.dirname(os.path.realpath('__file__')), 'config_pg_hsm_cp.ini')
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

email = {
    'user': conf.get('email', 'user'),
    'password': conf.get('email', 'password'),
    'host': conf.get('email', 'host'),
    'port': conf.getint('email', 'port'),
}

to = conf.get('email', 'to').split()
category = ['hair', 'pcc', 'laundry', 'oral', 'fem', 'baby', 'skin', 'br']
month_three = conf.get('pg_hsm', 'month_three')

pd_file_one = []
pd_file_two = []
sku_file = []
file = []

for each_category in category:
    pd_file_one.append(os.path.join(os.path.dirname(os.path.realpath('__file__')),
                                    ('货架特陈复核' + each_category + month_three + '.xlsx')))
    pd_file_two.append(os.path.join(os.path.dirname(os.path.realpath('__file__')),
                                    ('货架特陈复核_分包二_' + each_category + month_three + '.xlsx')))
    sku_file.append(os.path.join(os.path.dirname(os.path.realpath('__file__')),
                                 ('SKU复核' + each_category + month_three + '.xlsx')))
    file.append(os.path.join(os.path.dirname(os.path.realpath('__file__')),
                             ('pg_hsm_summary_' + each_category + month_three + '.xlsx')))

sql_get_store_info = """SELECT * FROM lenzbi.t_pg_report_hsm_address"""


def query_data_frame(db_dict, sql, result=True):
    with MySQLInstance(**db_dict, dict_result=result) as db:
        if db.query(sql):
            return DataFrame(db.query(sql))
        else:
            logger.info('No result.')
            sys.exit()


def main():
    subject = 'P&G_HSM_Checkpoint_' + month_three + '_' + datetime.now().strftime('%Y-%m-%d')
    contents = ['附件中为' + month_three + '全部对比复核数据', ]
    attachments = pd_file_one
    attachments.extend(pd_file_two)
    attachments.extend(sku_file)
    with EmailSender(**email) as email_sender:
        email_sender.send_email(to=to, subject=subject, contents=contents, attachments=attachments)

    store_info_df = query_data_frame(mysql_db_ppzck_task, sql_get_store_info)
    data = []
    for index1 in range(8):
        data.append(pd.read_excel(file[index1], category[index1]))
        data[index1] = data[index1][data[index1]['check_sku'] == 1]
        data[index1].drop_duplicates(subset='addressIDnum', inplace=True)
        store_info_df = pd.merge(store_info_df, data[index1].reindex(columns=['addressIDnum', 'rid']),
                                 how='inner', on='addressIDnum', sort=False, copy=False)

    result = pd.concat(data)
    result['shelf_url'] = 'http://pc.ppznet.com/task_pc//shenhe/aicorrect/images.jsp?responseid=' + result['rid']
    result_gb = result.groupby('addressIDnum').sum()
    result_gb.reset_index(inplace=True)
    result_gb['category'] = 'total'
    result = pd.concat([result, result_gb], sort=False)
    result['month'].fillna(method='ffill', inplace=True)
    store_info_df.drop(columns=store_info_df.columns[15:], inplace=True)
    result = pd.merge(store_info_df, result, how='inner', on='addressIDnum', sort=False, copy=False)

    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format(
        mysql_db_bi_task['username'], mysql_db_bi_task['password'], mysql_db_bi_task['host'], mysql_db_bi_task['port'],
        mysql_db_bi_task['schema'], 'utf8'))
    con = engine.connect()
    result.to_sql(name='t_pg_report_hsm_visualization', con=con, if_exists='replace', index=False)
    con.close()
    for i in range(8):
        os.remove(pd_file_one[i])
        os.remove(pd_file_two[i])
        os.remove(sku_file[i])
        os.remove(file[i])


if __name__ == '__main__':
    main()
    end_time = datetime.now()
    logger.info('time_consumed: %s' % (end_time-start_time))
