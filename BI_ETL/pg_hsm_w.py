#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import logging
import os
import configparser
import re
import pandas as pd
import numpy as np
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
conf_file = os.path.join(os.path.dirname(os.path.realpath('__file__')), 'config_pg_hsm_w.ini')
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

hnhb_list = ['Wuhan_Chuangjie', 'Shiyan_Xinhezuojingbao', 'Changsha_Jinguo', 'Hengyang_Jinguo',
             'Huaihua_Weierkang', 'Huanggang_Weierkang', 'Hunan_Yingshengxiang']
category = ['hair', 'pcc', 'laundry', 'oral', 'fem', 'baby', 'skin', 'br']

level = conf.getint('pg_hsm', 'level')
month = conf.get('pg_hsm', 'month')
year = conf.get('pg_hsm', 'year')
condition_one = conf.get('pg_hsm', 'condition_one')
condition_two = conf.get('pg_hsm', 'condition_two')
insert_order = conf.get('pg_hsm', 'insert_order').split()

sql_get_taskid = """SELECT DISTINCT taskid FROM lenzbi.t_pg_report_hsm_qnair WHERE `month` = %s AND category = '%s'"""

task = []
report_order = []
report_file = []

for each_category in category:
    with MySQLInstance(**mysql_db_ppzck_task, dict_result=False) as task_db:
        task.append(','.join(['\'' + y + '\'' for x in task_db.query(
            sql_get_taskid % (month, each_category)) for y in x]))
    report_order.append(conf.get('pg_hsm_' + each_category, 'report_order').split())
    report_file.append(os.path.join(os.path.dirname(os.path.realpath('__file__')),
                                    ('pg_hsm_report_' + each_category + '_' + month + '.xlsx')))

sql_delete_report = """DELETE FROM t_pg_report_hsm WHERE taskid IN (%s)"""

with MySQLInstance(**mysql_db_bi_task, dict_result=True) as delete_db:
    for index1 in range(8):
        delete_db.execute(sql_delete_report % task[index1])


def query_data_frame(db_dict, sql, result=True):
    with MySQLInstance(**db_dict, dict_result=result) as db:
        if db.query(sql):
            return DataFrame(db.query(sql))
        else:
            logger.info('No result.')
            sys.exit()


sql_get_store_info = """SELECT * FROM lenzbi.t_pg_report_hsm_address"""
sql_get_sku_info = """SELECT * FROM lenzbi.t_pg_report_hsm_sku"""
sql_get_qnair_info = """SELECT * FROM lenzbi.t_pg_report_hsm_qnair"""
sql_get_rid = """SELECT tr.id rid ,tr.taskid_owner taskid, tt.addressIDnum
FROM t_response tr LEFT JOIN t_tasklaunch tt ON tr.taskid = tt.taskid 
WHERE tr.taskid_owner IN (%s) 
%s 
AND tr.`status` NOT IN (%s)"""
sql_get_sku = """SELECT tr.Id rid, tispe.product_id, tispe.`status` 
FROM t_image_store_product_exist tispe LEFT JOIN t_response tr ON tispe.response_id = tr.id
WHERE tr.taskid_owner IN (%s) 
%s 
AND tr.`status` NOT IN (%s)"""
sql_get_answer = """SELECT tr.id rid, ta.qid, ta.answer, ta.image 
FROM t_answer ta LEFT JOIN t_response tr ON ta.response_id = tr.id
WHERE tr.taskid_owner IN (%s) 
%s 
AND tr.`status` NOT IN (%s)"""
sql_get_sku_verification = """SELECT tr.id rid, tre.username 
FROM t_response_examine tre LEFT JOIN t_response tr ON tr.Id = tre.id 
WHERE tr.taskid_owner IN (%s) 
%s 
AND tr.`status` NOT IN (%s) """
sql_get_multi_option = """SELECT tq.id qid, CONCAT('%s',tqo.option_value) option_name, tqo.option_index 
FROM t_question_option tqo LEFT JOIN t_question tq ON tqo.question_id = tq.Id
WHERE tq.taskid IN (%s)"""

rid_df = []

store_info_df = query_data_frame(mysql_db_ppzck_task, sql_get_store_info)
store_info_df['mark'] = store_info_df.apply(lambda x: x.addressIDnum[0], axis=1)
# store_info_df['mark'] = [x[0] for x in store_info_df['addressIDnum'].values]
sku_info_df = query_data_frame(mysql_db_ppzck_task, sql_get_sku_info)
qnair_info_df = query_data_frame(mysql_db_ppzck_task, sql_get_qnair_info)

for index2 in range(8):
    if index2 == 5 or index2 == 7:
        continue
    else:
        rid_df.append(query_data_frame(mysql_db_ppzck_task, sql_get_rid % (
            task[index2], condition_two, condition_one)))
        if index2 == 4 or index2 == 6:
            rid_df.append(rid_df[index2])

for index3 in range(8):
    rid_df[index3].drop_duplicates(subset='rid', inplace=True)
    store_info_df = pd.merge(store_info_df, rid_df[index3].reindex(columns=['addressIDnum', 'rid']),
                             how='left', on='addressIDnum', sort=False, copy=False)

store_info_df.drop_duplicates(subset='addressIDnum', inplace=True)
store_info_df.iloc[:, 16:] = store_info_df.iloc[:, 16:].apply(pd.notna)
store_info_df['category_num'] = store_info_df.iloc[:, 16:].apply(np.sum, axis=1)
store_info_df.drop(columns=store_info_df.columns[16:24], inplace=True)


def is_non_negative_number(num):
    pattern = re.compile(r'^[1-9]\d*\.\d+$|0\.\d+$')
    result = pattern.match(num)
    return True if result else num.isdigit()


def number_normalization_answer_new(criteria, answer):
    if pd.isna(answer) or answer == '':
        return 0
    elif criteria == 'criteria1':
        return float(answer) if is_non_negative_number(answer) else 0
    elif criteria == 'criteria2':
        if is_non_negative_number(answer):
            return float(answer) if float(answer) % 0.25 == 0 and float(answer) != 0.25 else 0
        else:
            return 0
    elif criteria == 'criteria3':
        if is_non_negative_number(answer):
            return float(answer) if float(answer) % 0.25 == 0 else 0
        else:
            return 0
    elif criteria == 'criteria4':
        if is_non_negative_number(answer):
            return float(answer) if float(answer) % 0.5 == 0 else 0
        else:
            return 0


def number_normalization_answer_status(criteria, answer):
    if pd.isna(answer) or answer == '':
        return 1
    elif criteria == 'criteria1':
        return 1 if is_non_negative_number(answer) else 0
    elif criteria == 'criteria2':
        if is_non_negative_number(answer):
            return 1 if float(answer) % 0.25 == 0 and float(answer) != 0.25 else 0
        else:
            return 0
    elif criteria == 'criteria3':
        if is_non_negative_number(answer):
            return 1 if float(answer) % 0.25 == 0 else 0
        else:
            return 0
    elif criteria == 'criteria4':
        if is_non_negative_number(answer):
            return 1 if float(answer) % 0.5 == 0 else 0
        else:
            return 0


def generate_number_df(df1, df2, i, wave):
    if i == 6:
        number_df = df1.set_index('rid')
        number_df['total_shelf'] = 0
        number_df['pg_shelf'] = 0
        number_df['total_display'] = 0
        number_df['pg_display'] = 0
        number_df['check_recent'] = 1
        return number_df, DataFrame()
    df = pd.merge(df1, qnair_info_df[(qnair_info_df['criteria'] != 'pic') & (
            qnair_info_df['criteria'] != 'multi_option') & (qnair_info_df['criteria'] != 'single_option') & (
                                             qnair_info_df['criteria'] != 'verification') & (
                                                 qnair_info_df['category'] == category[i])],
                  how='left', on='taskid', sort=False, copy=False)
    df = pd.merge(df, df2, how='left', on=['rid', 'qid'], sort=False, copy=False)
    df['answer_status'] = df.apply(lambda x: number_normalization_answer_status(x.criteria, x.answer), axis=1)
    df['answer_new'] = df.apply(lambda x: number_normalization_answer_new(x.criteria, x.answer), axis=1)
    check_number_df = df.reindex(columns=['addressIDnum', 'rid', 'taskid', 'qid',
                                          'title', 'answer', 'answer_status'])
    check_number_df = check_number_df[check_number_df['answer_status'] == 0]
    number_df = df.reindex(columns=['rid', 'addressIDnum', 'taskid', 'title', 'answer_new'])
    number_df = number_df.set_index(['rid', 'addressIDnum', 'taskid', 'title']).unstack()
    number_df.columns = number_df.columns.droplevel(0)
    number_df.reset_index(level=[1, 2], inplace=True)
    # 大小月之大月
    if wave == 0:
        number_df['shelf_main_brand_count'] = 0
        if i == 1:
            for each in [x.replace('_bath_lotion_shelf', '')
                         for x in number_df.columns.values if 'bath_lotion_shelf' in x]:
                number_df[each + '_shelf_distribution'] = 1 - (
                        (number_df[each + '_bath_lotion_shelf'] + number_df[each + '_soap_shelf'] +
                         number_df[each + '_hand_sanitizer_shelf']) == 0)
                number_df['shelf_main_brand_count'] = number_df['shelf_main_brand_count'] + number_df[
                    each + '_shelf_distribution']
        elif i == 2:
            number_df['downy_powder_shelf'] = 0
            number_df['bluemoon_powder_shelf'] = 0
            number_df['downy_bar_shelf'] = 0
            number_df['bluemoon_bar_shelf'] = 0
            for each in [x.replace('_powder_shelf', '') for x in number_df.columns.values if 'powder_shelf' in x]:
                number_df[each + '_shelf_distribution'] = 1 - (
                        (number_df[each + '_powder_shelf'] + number_df[each + '_liquid_shelf'] +
                         number_df[each + '_bar_shelf']) == 0)
                number_df['shelf_main_brand_count'] = number_df['shelf_main_brand_count'] + number_df[
                    each + '_shelf_distribution']
        elif i == 3:
            for each in [x.replace('_toothpaste_shelf', '')
                         for x in number_df.columns.values if 'toothpaste_shelf' in x]:
                number_df[each + '_shelf_distribution'] = 1 - (
                        (number_df[each + '_toothpaste_shelf'] + number_df[each + '_toothbrush_shelf']) == 0)
                number_df['shelf_main_brand_count'] = number_df['shelf_main_brand_count'] + number_df[
                    each + '_shelf_distribution']
        else:
            for each in [x for x in number_df.columns.values if 'shelf' in x and 'total_shelf' not in x]:
                number_df[each + '_distribution'] = 1 - (number_df[each] == 0)
                number_df['shelf_main_brand_count'] = number_df['shelf_main_brand_count'] + number_df[
                    each + '_distribution']
        number_df['main_brand_no_shelf_distribution'] = 0 + (number_df['shelf_main_brand_count'] == 0)

    agg_df = df.groupby(by=['rid', 'sub_title'])['answer_new'].sum().unstack()
    # 大小月之大月
    if wave == 0:
        if i == 1 or i == 2:
            agg_df['total_shelf'] = agg_df['total_shelf_A'] + agg_df['total_shelf_B'] + agg_df['total_shelf_C']
            agg_df['pg_shelf'] = agg_df['pg_shelf_A'] + agg_df['pg_shelf_B'] + agg_df['pg_shelf_C']
        if i == 3:
            agg_df['total_shelf'] = agg_df['total_shelf_A'] + agg_df['total_shelf_B']
            agg_df['pg_shelf'] = agg_df['pg_shelf_A'] + agg_df['pg_shelf_B']
    # 大小月之小月
    else:
        agg_df['total_shelf'] = 0
        agg_df['pg_shelf'] = 0

    agg_df['total_display'] = (agg_df['total_non_equity_display'] + agg_df['total_equity_display'] +
                               agg_df['total_endcap'] + agg_df['total_rack'] + agg_df['total_promotion_wall'] +
                               agg_df['total_basket'] + agg_df['total_scenario_heap'])
    agg_df['pg_display'] = (agg_df['pg_non_equity_display'] + agg_df['pg_equity_display'] +
                            agg_df['pg_endcap'] + agg_df['pg_rack'] + agg_df['pg_promotion_wall'] +
                            agg_df['pg_basket'] + agg_df['pg_scenario_heap'])
    agg_df['non_equity_display_distribution'] = 1 - (agg_df['total_non_equity_display'] == 0)
    agg_df['equity_display_distribution'] = 1 - (agg_df['total_equity_display'] == 0)
    agg_df['endcap_distribution'] = 1 - (agg_df['total_endcap'] == 0)
    agg_df['rack_distribution'] = 1 - (agg_df['total_rack'] == 0)
    agg_df['promotion_wall_distribution'] = 1 - (agg_df['total_promotion_wall'] == 0)
    agg_df['basket_distribution'] = 1 - (agg_df['total_basket'] == 0)
    agg_df['scenario_heap_distribution'] = 1 - (agg_df['total_scenario_heap'] == 0)
    agg_df['no_display_distribution'] = 0 + (agg_df['total_display'] == 0)
    agg_df['check_total_sum_non_equity_display'] = 0 + (
            agg_df['total_non_equity_display'] >= (agg_df['pg_non_equity_display'] +
                                                   agg_df['other_non_equity_display']))
    agg_df['check_total_sum_equity_display'] = 0 + (
            agg_df['total_equity_display'] >= (agg_df['pg_equity_display'] + agg_df['other_equity_display']))
    agg_df['check_total_sum_endcap'] = 0 + (
            agg_df['total_endcap'] >= (agg_df['pg_endcap'] + agg_df['other_endcap']))
    agg_df['check_total_sum_rack'] = 0 + (
            agg_df['total_rack'] >= (agg_df['pg_rack'] + agg_df['other_rack']))
    agg_df['check_total_sum_promotion_wall'] = 0 + (
            agg_df['total_promotion_wall'] >= (agg_df['pg_promotion_wall'] + agg_df['other_promotion_wall']))
    agg_df['check_total_sum_basket'] = 0 + (
            agg_df['total_basket'] >= (agg_df['pg_basket'] + agg_df['other_basket']))
    agg_df['check_total_sum_scenario_heap'] = 0 + (
            agg_df['total_scenario_heap'] >= (agg_df['pg_scenario_heap'] + agg_df['other_scenario_heap']))
    if i != 7:
        agg_df['check_total_sum_packing_column'] = 0 + (
                agg_df['total_packing_column'] >= (agg_df['pg_packing_column'] + agg_df['other_packing_column']))
    # 大小月之大月
    if wave == 0:
        if i == 1:
            agg_df['check_total_sum_bath_lotion_shelf'] = 0 + (
                    agg_df['total_shelf_A'] >= (agg_df['pg_shelf_A'] + agg_df['other_shelf_A'] - 0.00001))
            agg_df['check_total_sum_soap_shelf'] = 0 + (
                    agg_df['total_shelf_B'] >= (agg_df['pg_shelf_B'] + agg_df['other_shelf_B'] - 0.00001))
            agg_df['check_total_sum_hand_sanitizer_shelf'] = 0 + (
                    agg_df['total_shelf_C'] >= (agg_df['pg_shelf_C'] + agg_df['other_shelf_C'] - 0.00001))
            agg_df['check_shelf_endcap'] = agg_df.apply(lambda x: 0 if (
                    x.total_endcap > 0 and min(x.total_shelf_A, x.total_shelf_B, x.total_shelf_C) == 0) else 1, axis=1)
        elif i == 2:
            agg_df['check_total_sum_powder_shelf'] = 0 + (
                    agg_df['total_shelf_A'] >= (agg_df['pg_shelf_A'] + agg_df['other_shelf_A'] - 0.00001))
            agg_df['check_total_sum_liquid_shelf'] = 0 + (
                    agg_df['total_shelf_B'] >= (agg_df['pg_shelf_B'] + agg_df['other_shelf_B'] - 0.00001))
            agg_df['check_total_sum_bar_shelf'] = 0 + (
                    agg_df['total_shelf_C'] >= (agg_df['pg_shelf_C'] + agg_df['other_shelf_C'] - 0.00001))
            agg_df['check_shelf_endcap'] = agg_df.apply(lambda x: 0 if (
                    x.total_endcap > 0 and min(x.total_shelf_A, x.total_shelf_B, x.total_shelf_C) == 0) else 1, axis=1)
        elif i == 3:
            agg_df['check_total_sum_toothpaste_shelf'] = 0 + (
                    agg_df['total_shelf_A'] >= (agg_df['pg_shelf_A'] + agg_df['other_shelf_A'] - 0.00001))
            agg_df['check_total_sum_toothbrush_shelf'] = 0 + (
                    agg_df['total_shelf_B'] >= (agg_df['pg_shelf_B'] + agg_df['other_shelf_B'] - 0.00001))
            agg_df['check_shelf_endcap'] = agg_df.apply(lambda x: 0 if (
                    x.total_endcap > 0 and min(x.total_shelf_A, x.total_shelf_B) == 0) else 1, axis=1)
        else:
            agg_df['check_total_sum_shelf'] = 0 + (
                    agg_df['total_shelf'] >= (agg_df['pg_shelf'] + agg_df['other_shelf'] - 0.00001))
    # 大小月之小月
    else:
        if i == 1:
            agg_df['check_total_sum_bath_lotion_shelf'] = 1
            agg_df['check_total_sum_soap_shelf'] = 1
            agg_df['check_total_sum_hand_sanitizer_shelf'] = 1
            agg_df['check_shelf_endcap'] = 1
        elif i == 2:
            agg_df['check_total_sum_powder_shelf'] = 1
            agg_df['check_total_sum_liquid_shelf'] = 1
            agg_df['check_total_sum_bar_shelf'] = 1
            agg_df['check_shelf_endcap'] = 1
        elif i == 3:
            agg_df['check_total_sum_toothpaste_shelf'] = 1
            agg_df['check_total_sum_toothbrush_shelf'] = 1
            agg_df['check_shelf_endcap'] = 1
        else:
            agg_df['check_total_sum_shelf'] = 1

    if i == 1:
        agg_df['check_recent'] = 0 + (
                (agg_df['check_total_sum_bath_lotion_shelf'] + agg_df['check_total_sum_soap_shelf'] +
                 agg_df['check_total_sum_hand_sanitizer_shelf'] + agg_df['check_total_sum_non_equity_display'] +
                 agg_df['check_total_sum_equity_display'] + agg_df['check_total_sum_endcap'] +
                 agg_df['check_total_sum_rack'] + agg_df['check_total_sum_promotion_wall'] +
                 agg_df['check_total_sum_packing_column'] + agg_df['check_total_sum_basket'] +
                 agg_df['check_total_sum_scenario_heap'] + agg_df['check_shelf_endcap']) == 12)
    elif i == 2:
        agg_df['check_recent'] = 0 + (
                (agg_df['check_total_sum_powder_shelf'] + agg_df['check_total_sum_liquid_shelf'] +
                 agg_df['check_total_sum_bar_shelf'] + agg_df['check_total_sum_non_equity_display'] +
                 agg_df['check_total_sum_equity_display'] + agg_df['check_total_sum_endcap'] +
                 agg_df['check_total_sum_rack'] + agg_df['check_total_sum_promotion_wall'] +
                 agg_df['check_total_sum_packing_column'] + agg_df['check_total_sum_basket'] +
                 agg_df['check_total_sum_scenario_heap'] + agg_df['check_shelf_endcap']) == 12)
    elif i == 3:
        agg_df['check_recent'] = 0 + (
                (agg_df['check_total_sum_toothpaste_shelf'] + agg_df['check_total_sum_toothbrush_shelf'] +
                 agg_df['check_total_sum_non_equity_display'] + agg_df['check_total_sum_equity_display'] +
                 agg_df['check_total_sum_endcap'] + agg_df['check_total_sum_rack'] +
                 agg_df['check_total_sum_promotion_wall'] + agg_df['check_total_sum_packing_column'] +
                 agg_df['check_total_sum_basket'] + agg_df['check_total_sum_scenario_heap'] +
                 agg_df['check_shelf_endcap']) == 11)
    elif i == 7:
        agg_df['check_recent'] = 0 + (
                (agg_df['check_total_sum_shelf'] + agg_df['check_total_sum_non_equity_display'] +
                 agg_df['check_total_sum_equity_display'] + agg_df['check_total_sum_endcap'] +
                 agg_df['check_total_sum_rack'] + agg_df['check_total_sum_promotion_wall'] +
                 agg_df['check_total_sum_basket'] + agg_df['check_total_sum_scenario_heap']) == 8)
    else:
        agg_df['check_recent'] = 0 + (
                (agg_df['check_total_sum_shelf'] + agg_df['check_total_sum_non_equity_display'] +
                 agg_df['check_total_sum_equity_display'] + agg_df['check_total_sum_endcap'] +
                 agg_df['check_total_sum_rack'] + agg_df['check_total_sum_promotion_wall'] +
                 agg_df['check_total_sum_packing_column'] + agg_df['check_total_sum_basket'] +
                 agg_df['check_total_sum_scenario_heap']) == 9)
    number_df = number_df.join(agg_df)
    return number_df, check_number_df


def get_image_url(taskid, rid, qid, image):
    return '' if pd.isna(image) or image == '' else 'HYPERLINK("http://pc.ppznet.com/task_pc/images.jsp?year=' + year \
                                                   + '&taskid=' + taskid + '&responseid=' + rid + '&qid=' + qid \
                                                   + '","图片")'


def generate_image_df(df1, df2, i):
    df = pd.merge(df1, qnair_info_df[(qnair_info_df['criteria'] == 'pic') & (qnair_info_df['category'] == category[i])],
                  how='left', on='taskid', sort=False, copy=False)
    df = pd.merge(df, df2, how='left', on=['rid', 'qid'], sort=False, copy=False)
    df['image_url'] = df.apply(lambda x: get_image_url(x.taskid, x.rid, x.qid, x.image), axis=1)
    image_df = df.reindex(columns=['rid', 'category', 'month', 'title', 'image_url'])
    image_df = image_df.set_index(['rid', 'category', 'month', 'title']).unstack()
    image_df.columns = image_df.columns.droplevel(0)
    image_df.reset_index(level=[1, 2], inplace=True)
    return image_df


def sku_normalization_answer(rd, status, hnhb):
    if rd in hnhb_list and hnhb == 1:
        return -1
    else:
        return 0 if pd.isna(status) or status == 0 else 1


def fast_growing_sku_answer(answer_new, fast_growing):
    return 1 if answer_new == 1 and fast_growing == 1 else 0


def baby_p_target_sku(mark, a1, a2, ts):
    return ts-3 if mark == 'P' and (a1/ts < a2/(ts-3)) else ts


def fem_hnhb_target_sku(rd, mark, target_sku):
    if rd in hnhb_list:
        if mark == 'L':
            return 21
        elif mark == 'S':
            return 17
        elif mark == 'H':
            return 29
        else:
            return 23
    else:
        return target_sku


def generate_sku_df(df1, df2, i):
    df = pd.merge(df1, store_info_df, how='inner', on='addressIDnum', sort=False, copy=False)
    df = pd.merge(df, sku_info_df[sku_info_df['category'] == category[i]],
                  how='left', on='mark', sort=False, copy=False)
    df = pd.merge(df, df2, how='left', on=['rid', 'product_id'], sort=False, copy=False)
    df['answer_new'] = df.apply(lambda x: sku_normalization_answer(x.RD, x.status, x.hnhb), axis=1)
    df = df[df['answer_new'] != -1]
    if i == 4:
        df['target_sku'] = df.apply(lambda x: fem_hnhb_target_sku(x.RD, x.mark, x.target_sku), axis=1)
    df['fast_growing_sku_exist'] = df.apply(lambda x: fast_growing_sku_answer(x.answer_new, x.fast_growing), axis=1)
    sku_df = df.reindex(columns=['rid', 'mark', 'target_sku', 'product_name', 'answer_new'])
    sku_df = sku_df.set_index(['rid', 'mark', 'target_sku', 'product_name']).unstack()
    sku_df.columns = sku_df.columns.droplevel(0)
    sku_df.reset_index(level=[1, 2], inplace=True)
    agg_df = df.groupby('rid')['answer_new', 'fast_growing_sku_exist'].sum()
    agg_df.rename(columns={'answer_new': 'actual_sku',
                           'fast_growing_sku_exist': 'actual_fast_growing_sku'}, inplace=True)
    agg_df['base_sku'] = agg_df['actual_sku'] - agg_df['actual_fast_growing_sku']
    sku_df = sku_df.join(agg_df)
    if i == 5:
        agg1_df = df[df['denominator_option1'] != 0].groupby(
            by=['rid', 'denominator_option1']).agg({'answer_new': np.sum, 'numerator_option1': np.max})
        agg1_df['actual_sku'] = agg1_df.apply(lambda x: min(x.answer_new, x.numerator_option1), axis=1)
        agg1_df = agg1_df['actual_sku'].unstack()
        agg1_df['actual_sku1'] = agg1_df.apply(np.sum, axis=1)
        agg2_df = df[(df['denominator_option2'] != 0) & (df['mark'] != 'H')].groupby(
            by=['rid', 'denominator_option2']).agg({'answer_new': np.sum, 'numerator_option2': np.max})
        agg2_df['actual_sku'] = agg2_df.apply(lambda x: min(x.answer_new, x.numerator_option2), axis=1)
        agg2_df = agg2_df['actual_sku'].unstack()
        agg2_df['actual_sku2'] = agg2_df.apply(np.sum, axis=1)
        agg3_df = pd.concat([agg1_df['actual_sku1'], agg2_df['actual_sku2']], axis=1, sort=False)
        agg3_df['actual_sku3'] = np.fmax(agg3_df['actual_sku1'], agg3_df['actual_sku2'])
        sku_df = sku_df.join(agg3_df)
        sku_df['actual_sku'] = sku_df['actual_sku3']
        sku_df['target_sku3'] = sku_df.apply(lambda x: baby_p_target_sku(
            x.mark, x.actual_sku1, x.actual_sku2, x.target_sku), axis=1)
        sku_df['target_sku'] = sku_df['target_sku3']
        sku_df.drop(columns=['actual_sku1', 'actual_sku2', 'actual_sku3', 'target_sku3'], inplace=True)
    sku_df['fast_growing_sku_compliance'] = np.where(
        sku_df['actual_sku'] == 0, 0, round(sku_df['actual_fast_growing_sku'] / sku_df['actual_sku'], 4))
    del sku_df['mark']
    return sku_df


def sku_verification_normalization_answer(username, i):
    if pd.isna(username) or username == '':
        return 0
    else:
        name = username.split(';')
        if len(name) == 1:
            name.append('')
        if i == 4:
            return ('N' in name[0]) + ('N' in name[1])
        elif i == 5:
            return ('Y' in name[0]) + ('Y' in name[1])
        elif i == 6:
            return ('H' in name[0]) + ('H' in name[1])
        elif i == 7:
            return ('T' in name[0]) + ('T' in name[1])
        else:
            return len(username.split(';'))


def generate_verification_df(df1, df2, df3, i):
    if i == 6:
        df = df1.set_index('rid')
        df['shelf_display_verification_1'] = 1
        df['shelf_display_verification_2'] = 1
        del df['addressIDnum']
        del df['taskid']
    else:
        df = pd.merge(df1, qnair_info_df[(qnair_info_df['criteria'] == 'verification') & (
                qnair_info_df['category'] == category[i])], how='left', on='taskid', sort=False, copy=False)
        df = pd.merge(df, df2, how='left', on=['rid', 'qid'], sort=False, copy=False)
        df = df.reindex(columns=['rid', 'title', 'answer'])
        df = df.set_index(['rid', 'title']).unstack()
        df.columns = df.columns.droplevel(0)
        df['shelf_display_verification_1'] = 1 - pd.isna(df.person_verification_2) - (df.person_verification_2 == '')
        # df['shelf_display_verification_1'] = df.apply(
        #     lambda x: 1 - pd.isna(x.person_verification_2) - (x.person_verification_2 == ''), axis=1)
        df['shelf_display_verification_2'] = 1 - pd.isna(df.person_verification_3) - (df.person_verification_3 == '')
    df3['sku_verification_count'] = df3.apply(lambda x: sku_verification_normalization_answer(x.username, i), axis=1)
    verification_df = df3.set_index('rid')
    verification_df = df.join(verification_df)
    verification_df['sku_verification_count'].fillna(0, inplace=True)
    return verification_df


def multi_option_normalization_answer(answer, option_index):
    if pd.isna(answer):
        return 0
    else:
        return 1 if option_index in answer.split(',') else 0


def generate_multi_option_df(df1, df2, df3, i):
    if i == 0:
        df = pd.merge(df1, qnair_info_df[(qnair_info_df['criteria'] == 'multi_option') & (
                qnair_info_df['category'] == category[i])], how='left', on='taskid', sort=False, copy=False)
        df = pd.merge(df, df2, how='left', on=['rid', 'qid'], sort=False, copy=False)
        df = pd.merge(df, df3, how='left', on='qid', sort=False, copy=False)
        df['answer_new'] = df.apply(lambda x: multi_option_normalization_answer(x.answer, x.option_index), axis=1)
        multi_option_df = df.reindex(columns=['rid', 'option_name', 'answer_new'])
        multi_option_df = multi_option_df.set_index(['rid', 'option_name']).unstack()
        multi_option_df.columns = multi_option_df.columns.droplevel(0)
        del multi_option_df['共同陈列_以上都没有']
        multi_option_df['共同陈列_以上都没有'] = 1 - multi_option_df.apply(np.max, axis=1)
        return multi_option_df
    else:
        return DataFrame()


def generate_single_option_df(df1, df2, i):
    if i == 7:
        df = pd.merge(df1, qnair_info_df[(qnair_info_df['criteria'] == 'single_option') & (
                qnair_info_df['category'] == category[i])], how='left', on='taskid', sort=False, copy=False)
        df = pd.merge(df, df2, how='left', on=['rid', 'qid'], sort=False, copy=False)
        df['answer_new'] = df.apply(
            lambda x: '否' if pd.isna(x.answer) or x.answer == '' or int(x.answer) != 0 else '是', axis=1)
        single_option_df = df.reindex(columns=['rid', 'title', 'answer_new'])
        single_option_df = single_option_df.set_index(['rid', 'title']).unstack()
        single_option_df.columns = single_option_df.columns.droplevel(0)
        return single_option_df
    else:
        return DataFrame()


def to_result(rd, sd, ad, svd, mod, i, wave):
    number_df, check_number_df = generate_number_df(rd, ad, i, wave)
    image_df = generate_image_df(rd, ad, i)
    sku_df = generate_sku_df(rd, sd, i)
    verification_df = generate_verification_df(rd, ad, svd, i)
    multi_option_df = generate_multi_option_df(rd, ad, mod, i)
    single_option_df = generate_single_option_df(rd, ad, i)
    result_df = number_df.join(image_df)
    result_df = result_df.join(sku_df)
    result_df = result_df.join(verification_df)
    result_df = result_df.join(multi_option_df)
    result_df = result_df.join(single_option_df)
    result_df.reset_index(inplace=True)
    if i == 7:
        result_df['br_whole_store_distribution'] = 1 - (
                (result_df['total_shelf'] + result_df['total_display'] + result_df['actual_sku']) == 0)
        result_df['gillette_whole_store_distribution'] = 1 - (
                (result_df['pg_shelf'] + result_df['pg_display'] + result_df['actual_sku']) == 0)
    return result_df, check_number_df


sql_insert_report = """INSERT INTO t_pg_report_hsm (rid,addressIDnum,`month`,category,actual_sku,target_sku,
pg_display,total_display,pg_shelf,total_shelf,actual_fast_growing_sku,fast_growing_sku_compliance,base_sku,taskid,
category_num) VALUES """


def to_report(i):
    logger.info('Run PID (%s)...' % os.getpid())
    sku_df = query_data_frame(mysql_db_ppzck_task, sql_get_sku % (task[i], condition_two, condition_one))
    answer_df = query_data_frame(mysql_db_ppzck_task, sql_get_answer % (task[i], condition_two, condition_one))
    sku_verification_df = query_data_frame(mysql_db_ppzck_task, sql_get_sku_verification % (
        task[i], condition_two, condition_one))
    sku_df.drop_duplicates(subset=['rid', 'product_id'], inplace=True)
    answer_df.drop_duplicates(subset=['rid', 'qid'], inplace=True)
    sku_verification_df.drop_duplicates(subset='rid', inplace=True)
    if i == 0:
        multi_option_df = query_data_frame(mysql_db_ppzck_task, sql_get_multi_option % ('共同陈列_', task[i]))
        multi_option_df.drop_duplicates(subset=['qid', 'option_index'], inplace=True)
    else:
        multi_option_df = DataFrame()
    result_df, check_number_df = to_result(
        rid_df[i], sku_df, answer_df, sku_verification_df, multi_option_df, i, level)
    df = pd.merge(result_df, store_info_df, how='inner', on='addressIDnum')
    report_df = df.reindex(columns=report_order[i])
    writer = pd.ExcelWriter(report_file[i])
    report_df.to_excel(writer, category[i], index=False)
    check_number_df.to_excel(writer, category[i] + '_number', index=False)
    writer.close()
    with MySQLInstance(**mysql_db_bi_task, dict_result=True) as db:
        db.executemany(sql_insert_report + '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                       [tuple(x) for x in df.reindex(columns=insert_order).values])


if __name__ == '__main__':
    logger.info('Parent process %s.' % os.getpid())
    p = Pool(2)
    for index in range(8):
        p.apply_async(to_report, args=(index,))
    p.close()
    p.join()
    end_time = datetime.now()
    logger.info('time_consumed: %s' % (end_time-start_time))
