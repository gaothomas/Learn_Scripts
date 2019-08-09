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

start_time = datetime.now()

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
log_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'log_pg_hsm_1907.log')
f_handler = logging.FileHandler(log_file)
f_handler.setLevel(logging.INFO)
formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
f_handler.setFormatter(formatter)
logger.addHandler(f_handler)
s_handler = logging.StreamHandler()
s_handler.setLevel(logging.DEBUG)
logger.addHandler(s_handler)

conf = configparser.ConfigParser()
conf_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config_pg_hsm_1907.ini')
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

category = conf.get('pg_hsm', 'category').split()
task_new = []
for each_category in category:
    task_new.append(conf.get('pg_hsm_' + each_category, 'task_new'))

sql_data = """SELECT rid, addressIDnum, `month`, category, actual_sku, target_sku, 
actual_fast_growing_sku, fast_growing_sku_compliance 
FROM t_pg_report_hsm_%s WHERE taskid IN (%s) AND check_sku = '1' AND category = '%s'"""
sql_get_store_info = """SELECT * FROM lenzbi.t_pg_report_hsm_address"""
sql_get_count = """SELECT count(0)/9 count FROM t_pg_report_hsm_visualization"""
sql_delete_count = """DELETE FROM t_pg_report_hsm_count WHERE date = %s"""
sql_insert_count = """INSERT INTO t_pg_report_hsm_count (date, count) VALUES (%s,%s)"""


def query_data_frame(db_dict, sql, result=True):
    with MySQLInstance(**db_dict, dict_result=result) as db:
        if db.query(sql):
            return DataFrame(db.query(sql))
        else:
            logger.info('No result.')
            sys.exit()


def main():
    store_info_df = query_data_frame(mysql_db_ppzck_task, sql_get_store_info)
    data = []

    for index1 in range(8):
        logger.info(sql_data % (category[index1], task_new[index1], category[index1]))
        data.append(query_data_frame(mysql_db_bi_task, sql_data % (category[index1],
                                                                   task_new[index1], category[index1])))
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
    with MySQLInstance(**mysql_db_bi_task, dict_result=False) as db:
        count = int(db.query(sql_get_count)[0][0])
        date = datetime.now().strftime('%Y%m%d')
        db.execute(sql_delete_count % date)
        db.execute(sql_insert_count % (date, count))


if __name__ == '__main__':
    logger.info('Parent process %s.' % os.getpid())
    main()
    end_time = datetime.now()
    logger.info('time_consumed: %s' % (end_time-start_time))
