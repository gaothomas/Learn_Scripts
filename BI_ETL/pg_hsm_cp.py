#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import logging
import os
import configparser
import pandas as pd
from pandas import DataFrame
from datetime import datetime
from multiprocessing import Pool
from MySQLManager import MySQLInstance

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

category = ['hair', 'pcc', 'laundry', 'oral', 'fem', 'baby', 'skin', 'br']
level = conf.getint('pg_hsm', 'level')
month_one = conf.get('pg_hsm', 'month_one')
month_two = conf.get('pg_hsm', 'month_two')
month_three = conf.get('pg_hsm', 'month_three')
insert_order = conf.get('pg_hsm', 'insert_order').split()
sku_order = conf.get('pg_hsm', 'sku_order').split()
report_path = os.path.join(os.path.dirname(os.path.realpath('__file__')), ('pg_hsm_report_' + month_three + '.xlsx'))

sql_get_taskid = """SELECT DISTINCT taskid FROM lenzbi.t_pg_report_hsm_qnair WHERE `month` = %s AND category = '%s'"""
sql_get_taskname = """SELECT id taskid, taskname FROM t_task WHERE id IN (%s)"""
sql_data = """SELECT * FROM t_pg_report_hsm WHERE taskid IN (%s) AND category = '%s'"""

task_one = []
task_two = []
task_three = []
pd_order = []
pd_file_one = []
pd_file_two = []
sku_file = []
file = []

for each_category in category:
    with MySQLInstance(**mysql_db_ppzck_task, dict_result=False) as task_db:
        task_one.append(','.join(['\'' + y + '\'' for x in task_db.query(
            sql_get_taskid % (month_one, each_category)) for y in x]))
        task_two.append(','.join(['\'' + y + '\'' for x in task_db.query(
            sql_get_taskid % (month_two, each_category)) for y in x]))
        task_three.append(','.join(['\'' + y + '\'' for x in task_db.query(
            sql_get_taskid % (month_three, each_category)) for y in x]))
    pd_order.append(conf.get('pg_hsm_' + each_category, 'pd_order').split())
    pd_file_one.append(os.path.join(os.path.dirname(os.path.realpath('__file__')),
                                    ('货架特陈复核' + each_category + month_three + '.xlsx')))
    pd_file_two.append(os.path.join(os.path.dirname(os.path.realpath('__file__')),
                                    ('货架特陈复核_分包二_' + each_category + month_three + '.xlsx')))
    sku_file.append(os.path.join(os.path.dirname(os.path.realpath('__file__')),
                                 ('SKU复核' + each_category + month_three + '.xlsx')))
    file.append(os.path.join(os.path.dirname(os.path.realpath('__file__')),
                             ('pg_hsm_summary_' + each_category + month_three + '.xlsx')))


def query_data_frame(db_dict, sql, result=True):
    with MySQLInstance(**db_dict, dict_result=result) as db:
        if db.query(sql):
            return DataFrame(db.query(sql))
        else:
            logger.info('No result.')
            sys.exit()


def check_vs_pp_total_shelf(new, old):
    if pd.isna(old) or (new == 0 and old == 0):
        return 1
    elif new == 0 or old == 0:
        return 0
    else:
        return 1 if abs(new-old)/new <= 0.2 else 0


def check_vs_pp_pg_shelf(new, old):
    if pd.isna(old) or (new == 0 and old == 0):
        return 1
    elif new == 0 or old == 0:
        return 0
    else:
        return 1 if abs(new-old)/new <= 0.2 else 0


def check_vs_pp_pg_display(nt, npg, ot, opg):
    if pd.isna(ot) or (nt == 0 and ot == 0):
        return 1
    elif nt == 0 or ot == 0:
        return 0
    elif npg == 0 and opg == 0:
        return 1
    elif npg == 0 or opg == 0:
        return 0
    else:
        return 1 if abs((npg/nt)-(opg/ot))/(npg/nt) <= 0.4 else 0


def check_vs_pp_sku(i, new, old):
    if pd.isna(old) or (new == 0 and old == 0):
        return 1
    elif new == 0 or old == 0:
        return 0
    elif i == 0:
        return 1 if abs(new-old)/new <= 0.115 else 0
    elif i == 1:
        return 1 if abs(new-old)/new <= 0.13 else 0
    elif i == 2:
        return 1 if abs(new-old)/new <= 0.17 else 0
    elif i == 3:
        return 1 if abs(new-old)/new <= 0.14 else 0
    elif i == 4:
        return 1 if abs(new-old)/new <= 0.24 else 0
    elif i == 5:
        return 1 if abs(new-old)/new <= 0.30 else 0
    elif i == 6:
        return 1 if abs(new-old)/new <= 0.22 else 0
    else:
        return 1 if abs(new-old)/new <= 0.29 else 0


def check_all(cr, sdv1, sdv2, svc, cts, cps, cpd, cpsku):
    if sdv1 == 0 or svc == 0:
        return 0
    elif cr == 1 and ((cts + cps + cpd) == 3 or sdv2 == 1) and (cpsku == 1 or svc == 2):
        return 1
    else:
        return 0


def check_sku(svc, cpsku):
    if svc == 0:
        return 0
    elif cpsku == 1 or svc == 2:
        return 1
    else:
        return 0


def check_vs_pp_total_shelf_v(sdv1, sdv2, cts):
    if sdv1 == 0:
        return '漏审'
    elif sdv2 == 1:
        return '已复核'
    elif cts == 1:
        return '无需复核'
    else:
        return '复核'


def check_vs_pp_pg_shelf_v(sdv1, sdv2, cps):
    if sdv1 == 0:
        return '漏审'
    elif sdv2 == 1:
        return '已复核'
    elif cps == 1:
        return '无需复核'
    else:
        return '复核'


def check_vs_pp_pg_display_v(sdv1, sdv2, cpd):
    if sdv1 == 0:
        return '漏审'
    elif sdv2 == 1:
        return '已复核'
    elif cpd == 1:
        return '无需复核'
    else:
        return '复核'


def check_sku_v(svc, cpsku):
    if svc == 0:
        return '漏审'
    elif svc == 2:
        return '已复核'
    elif cpsku == 1:
        return '无需复核'
    else:
        return '复核'


def to_result(i):
    logger.info('Run PID (%s)...' % os.getpid())
    df1 = query_data_frame(mysql_db_bi_task, sql_data % (task_one[i], category[i]))
    df1.drop_duplicates(subset='addressIDnum', inplace=True)
    df1['total_shelf'] = df1['total_shelf'].astype(float)
    df1['pg_shelf'] = df1['pg_shelf'].astype(float)
    df2 = query_data_frame(mysql_db_bi_task, sql_data % (task_two[i], category[i]))
    df2.drop_duplicates(subset='addressIDnum', inplace=True)
    df2['total_display'] = df2['total_display'].astype(float)
    df2['pg_display'] = df2['pg_display'].astype(float)
    df3 = pd.read_excel(report_path, category[i])
    taskname_df = query_data_frame(mysql_db_ppzck_task, sql_get_taskname % task_three[i])
    df3 = pd.merge(df3, taskname_df, how='left', on='taskid')
    df3 = pd.merge(df3, df2, how='left', on='addressIDnum', suffixes=('', '_two'))
    df3 = pd.merge(df3, df1, how='left', on='addressIDnum', suffixes=('', '_one'))
    df3['复核链接'] = ('=HYPERLINK("http://pc.ppznet.com/task_pc/jsp/oldanswer.jsp?responseid=' +
                   df3['rid'] + '","点击此处")')
    df3['本月链接'] = ('=HYPERLINK("http://pc.ppznet.com/task_pc//getManualReview.action?taskid=' +
                   df3['taskid'] + '&responseid=' + df3['rid'] + '&addressidnum=' +
                   df3['addressIDnum'] + '&iffenqu=0","图片")')
    df3['上月链接'] = ('=HYPERLINK("http://pc.ppznet.com/task_pc//getManualReview.action?taskid=' +
                   df3['taskid_two'] + '&responseid=' + df3['rid_two'] + '&addressidnum=' +
                   df3['addressIDnum'] + '&iffenqu=0","图片")')
    df3.loc[pd.isna(df3['rid_two']), '上月链接'] = ''
    df3['segmentation'] = df3['taskname'].apply(lambda x: 1 if '分包二' in x else 0)
    if level == 0:
        df3['check_total_shelf'] = df3.apply(
            lambda x: check_vs_pp_total_shelf(x.total_shelf, x.total_shelf_one), axis=1)
        df3['check_pg_shelf'] = df3.apply(lambda x: check_vs_pp_pg_shelf(x.pg_shelf, x.pg_shelf_one), axis=1)
    else:
        df3['check_total_shelf'] = 1
        df3['check_pg_shelf'] = 1
    df3['check_pg_display'] = df3.apply(lambda x: check_vs_pp_pg_display(
        x.total_display, x.pg_display, x.total_display_two, x.pg_display_two), axis=1)
    df3['check_pg_sku'] = df3.apply(lambda x: check_vs_pp_sku(i, x.actual_sku, x.actual_sku_two), axis=1)
    df3['check_all'] = df3.apply(lambda x: check_all(
        x.check_recent, x.shelf_display_verification_1, x.shelf_display_verification_2, x.sku_verification_count,
        x.check_total_shelf, x.check_pg_shelf, x.check_pg_display, x.check_pg_sku), axis=1)
    df3['check_sku'] = df3.apply(lambda x: check_sku(x.sku_verification_count, x.check_pg_sku), axis=1)
    df3['总货架组数两期变化超过20%'] = df3.apply(lambda x: check_vs_pp_total_shelf_v(
        x.shelf_display_verification_1, x.shelf_display_verification_2, x.check_total_shelf), axis=1)
    df3['P&G货架组数两期变化超过20%'] = df3.apply(lambda x: check_vs_pp_pg_shelf_v(
        x.shelf_display_verification_1, x.shelf_display_verification_2, x.check_pg_shelf), axis=1)
    df3['P&G特殊陈列占比两期变化超过40%'] = df3.apply(lambda x: check_vs_pp_pg_display_v(
        x.shelf_display_verification_1, x.shelf_display_verification_2, x.check_pg_display), axis=1)
    df3['是否复核'] = df3.apply(lambda x: check_sku_v(x.sku_verification_count, x.check_pg_sku), axis=1)
    df3.reindex(columns=insert_order).to_excel(file[i], category[i], index=False)
    check_columns = [x for x in df3.columns if 'check_total_sum' in x or 'check_shelf_endcap' in x]
    df3[check_columns] = df3[check_columns].applymap(lambda x: '有问题' if x == 0 else 1)
    columns_transform = {'check_total_sum_shelf': '货架',
                         'check_total_sum_bath_lotion_shelf': '沐浴露货架',
                         'check_total_sum_soap_shelf': '香皂货架',
                         'check_total_sum_hand_sanitizer_shelf': '洗手液货架',
                         'check_total_sum_powder_shelf': '洗衣粉货架',
                         'check_total_sum_liquid_shelf': '洗衣液货架',
                         'check_total_sum_bar_shelf': '洗衣皂货架',
                         'check_total_sum_toothpaste_shelf': '牙膏货架',
                         'check_total_sum_toothbrush_shelf': '牙刷货架',
                         'check_total_sum_non_equity_display': '非形象地堆',
                         'check_total_sum_equity_display': '形象地堆',
                         'check_total_sum_endcap': '端架',
                         'check_total_sum_rack': '陈列架',
                         'check_total_sum_promotion_wall': '促销墙',
                         'check_total_sum_basket': '框篮',
                         'check_total_sum_scenario_heap': '情景堆',
                         'check_total_sum_packing_column': '包柱',
                         'content_verification_2': '一审备注',
                         'content_verification_3': '二审备注',
                         'person_verification_2': '一审人员姓名',
                         'person_verification_3': '二审人员姓名',
                         'check_shelf_endcap': '货架分品类为0但端架有组数',
                         'taskname': '任务名称',
                         'addressIDnum': '地址编号'}
    df3.rename(columns=columns_transform, inplace=True)
    pd_df_one = df3[df3['segmentation'] == 0].reindex(columns=pd_order[i])
    pd_df_two = df3[df3['segmentation'] == 1].reindex(columns=pd_order[i])
    sku_df = df3.reindex(columns=sku_order)
    pd_df_one.to_excel(pd_file_one[i], category[i], index=False)
    pd_df_two.to_excel(pd_file_two[i], category[i], index=False)
    sku_df.to_excel(sku_file[i], category[i], index=False)


if __name__ == '__main__':
    logger.info('Parent process %s.' % os.getpid())
    p = Pool(4)
    for index in range(8):
        p.apply_async(to_result, args=(index,))
    p.close()
    p.join()
    end_time = datetime.now()
    logger.info('time_consumed: %s' % (end_time-start_time))
