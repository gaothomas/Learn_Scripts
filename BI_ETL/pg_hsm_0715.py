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
from EmailSender import EmailSender

start_time = datetime.now()

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
log_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'log_pg_hsm.log')
f_handler = logging.FileHandler(log_file)
f_handler.setLevel(logging.INFO)
formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
f_handler.setFormatter(formatter)
logger.addHandler(f_handler)
s_handler = logging.StreamHandler()
s_handler.setLevel(logging.DEBUG)
logger.addHandler(s_handler)

conf = configparser.ConfigParser()
conf_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config_pg_hsm.ini')
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

status_not_in_new = conf.get('pg_hsm', 'status_not_in_new')
time_selection_new = conf.get('pg_hsm', 'time_selection_new')
status_not_in_old = conf.get('pg_hsm', 'status_not_in_old')
time_selection_old = conf.get('pg_hsm', 'time_selection_old')
hnhb_list = conf.get('pg_hsm', 'hnhb_list').split()
insert_table_order_new = conf.get('pg_hsm', 'insert_table_order_new').split()
insert_table_order_old = conf.get('pg_hsm', 'insert_table_order_old').split()
category = conf.get('pg_hsm', 'category').split()

task_new = []
task_old = []
year_new = []
year_old = []
report_order = []
checkpoint_order = []
new_file = []
old_file = []
new_checkpoint_file = []
old_checkpoint_file = []
new_checkpoint_number_file = []
old_checkpoint_number_file = []


for each_category in category:
    task_new.append(conf.get('pg_hsm_' + each_category, 'task_new'))
    task_old.append(conf.get('pg_hsm_' + each_category, 'task_old'))
    year_new.append(conf.get('pg_hsm_' + each_category, 'year_new'))
    year_old.append(conf.get('pg_hsm_' + each_category, 'year_old'))
    report_order.append(conf.get('pg_hsm_' + each_category, 'report_order').split())
    checkpoint_order.append(conf.get('pg_hsm_' + each_category, 'checkpoint_order').split())
    new_file.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                 ('pg_hsm_report_new_' + each_category +
                                  datetime.now().strftime('%Y-%m-%d') + '.csv')))
    old_file.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                 ('pg_hsm_report_old_' + each_category +
                                  datetime.now().strftime('%Y-%m-%d') + '.csv')))
    new_checkpoint_file.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                            ('pg_hsm_report_new_checkpoint_' + each_category +
                                             datetime.now().strftime('%Y-%m-%d') + '.csv')))
    old_checkpoint_file.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                            ('pg_hsm_report_old_checkpoint_' + each_category +
                                             datetime.now().strftime('%Y-%m-%d') + '.csv')))
    new_checkpoint_number_file.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                   ('pg_hsm_report_new_checkpoint_number_' + each_category +
                                                    datetime.now().strftime('%Y-%m-%d') + '.csv')))
    old_checkpoint_number_file.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                   ('pg_hsm_report_old_checkpoint_number_' + each_category +
                                                    datetime.now().strftime('%Y-%m-%d') + '.csv')))

sql_delete_report = """DELETE FROM t_pg_report_hsm WHERE taskid IN (%s)"""
sql_delete_sku = """DELETE FROM t_pg_report_hsm_sku_details WHERE taskid IN (%s)"""

with MySQLInstance(**mysql_db_bi_task, dict_result=True) as delete_db:
    for index1 in range(8):
        if index1 == 5 or index1 == 7:
            continue
        else:
            delete_db.execute(sql_delete_report % task_new[index1])
            delete_db.execute(sql_delete_sku % task_new[index1])
            delete_db.execute(sql_delete_report % task_old[index1])
            delete_db.execute(sql_delete_sku % task_old[index1])


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

rid_df_list_new = []
rid_df_list_old = []

store_info_df = query_data_frame(mysql_db_ppzck_task, sql_get_store_info)
store_new_df = store_info_df.copy()
store_old_df = store_info_df.copy()
store_info_df['mark'] = store_info_df.apply(lambda x: x.addressIDnum[0], axis=1)
# store_info_df['mark'] = [x[0] for x in store_info_df['addressIDnum'].values]
sku_info_df = query_data_frame(mysql_db_ppzck_task, sql_get_sku_info)
qnair_info_df = query_data_frame(mysql_db_ppzck_task, sql_get_qnair_info)

for index2 in range(8):
    if index2 == 5 or index2 == 7:
        continue
    else:
        rid_df_list_new.append(query_data_frame(mysql_db_ppzck_task, sql_get_rid % (
            task_new[index2], time_selection_new, status_not_in_new)))
        rid_df_list_old.append(query_data_frame(mysql_db_ppzck_task, sql_get_rid % (
            task_old[index2], time_selection_old, status_not_in_old)))
        if index2 == 4 or index2 == 6:
            rid_df_list_new.append(rid_df_list_new[index2])
            rid_df_list_old.append(rid_df_list_old[index2])

for index3 in range(8):
    rid_df_list_new[index3].drop_duplicates(subset='rid', inplace=True)
    rid_df_list_old[index3].drop_duplicates(subset='rid', inplace=True)
    store_new_df = pd.merge(store_new_df, rid_df_list_new[index3].reindex(columns=['addressIDnum', 'rid']),
                            how='left', on='addressIDnum', sort=False, copy=False)
    store_old_df = pd.merge(store_old_df, rid_df_list_old[index3].reindex(columns=['addressIDnum', 'rid']),
                            how='left', on='addressIDnum', sort=False, copy=False)
store_new_df.drop_duplicates(subset='addressIDnum', inplace=True)
store_new_df.iloc[:, 15:] = store_new_df.iloc[:, 15:].apply(pd.notna)
store_new_df['category_num'] = store_new_df.iloc[:, 15:].apply(np.sum, axis=1)
store_new_df.drop(columns=store_new_df.columns[15:23], inplace=True)
store_old_df.drop_duplicates(subset='addressIDnum', inplace=True)
store_old_df.iloc[:, 15:] = store_old_df.iloc[:, 15:].apply(pd.notna)
store_old_df['category_num'] = store_old_df.iloc[:, 15:].apply(np.sum, axis=1)
store_old_df.drop(columns=store_old_df.columns[15:23], inplace=True)


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


def generate_number_df(df1, df2, i):
    df = pd.merge(df1, qnair_info_df[(qnair_info_df['criteria'] != 'pic') & (
            qnair_info_df['criteria'] != 'multi_option') & (qnair_info_df['criteria'] != 'single_option') & (
            qnair_info_df['criteria'] != 'verification') & (qnair_info_df['category'] == category[i])],
                  how='left', on='taskid', sort=False, copy=False)
    df = pd.merge(df, df2, how='left', on=['rid', 'qid'], sort=False, copy=False)
    df['answer_status'] = df.apply(lambda x: number_normalization_answer_status(x.criteria, x.answer), axis=1)
    df['answer_new'] = df.apply(lambda x: number_normalization_answer_new(x.criteria, x.answer), axis=1)
    check_number_df = df.reindex(columns=['addressIDnum', 'rid', 'taskid', 'qid', 'title', 'answer', 'answer_status'])
    check_number_df = check_number_df[check_number_df['answer_status'] == 0]
    number_df = df.reindex(columns=['rid', 'category', 'month', 'addressIDnum', 'taskid', 'title', 'answer_new'])
    number_df = number_df.set_index(['rid', 'category', 'month', 'addressIDnum', 'taskid', 'title']).unstack()
    number_df.columns = number_df.columns.droplevel(0)
    number_df.reset_index(level=[1, 2, 3, 4], inplace=True)
    number_df['shelf_main_brand_count'] = 0
    if i == 1:
        for each in [x.replace('_bath_lotion_shelf', '') for x in number_df.columns.values if 'bath_lotion_shelf' in x]:
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
        for each in [x.replace('_toothpaste_shelf', '') for x in number_df.columns.values if 'toothpaste_shelf' in x]:
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
    if i == 1 or i == 2:
        agg_df['total_shelf'] = agg_df['total_shelf_A'] + agg_df['total_shelf_B'] + agg_df['total_shelf_C']
        agg_df['pg_shelf'] = agg_df['pg_shelf_A'] + agg_df['pg_shelf_B'] + agg_df['pg_shelf_C']
    if i == 3:
        agg_df['total_shelf'] = agg_df['total_shelf_A'] + agg_df['total_shelf_B']
        agg_df['pg_shelf'] = agg_df['pg_shelf_A'] + agg_df['pg_shelf_B']
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


def get_image_url(taskid, rid, qid, year, image):
    return '' if pd.isna(image) or image == '' else '=HYPERLINK("http://pc.ppznet.com/task_pc/images.jsp?year=' + year \
                                                   + '&taskid=' + taskid + '&responseid=' + rid + '&qid=' + qid \
                                                   + '","图片")'


def generate_image_df(df1, df2, year, i):
    df = pd.merge(df1, qnair_info_df[(qnair_info_df['criteria'] == 'pic') & (qnair_info_df['category'] == category[i])],
                  how='left', on='taskid', sort=False, copy=False)
    df = pd.merge(df, df2, how='left', on=['rid', 'qid'], sort=False, copy=False)
    df['image_url'] = df.apply(lambda x: get_image_url(x.taskid, x.rid, x.qid, year, x.image), axis=1)
    image_df = df.reindex(columns=['rid', 'title', 'image_url'])
    image_df = image_df.set_index(['rid', 'title']).unstack()
    image_df.columns = image_df.columns.droplevel(0)
    return image_df


sql_insert_sku = """INSERT INTO t_pg_report_hsm_sku_details (rid,product_id,product_name,is_exist,taskid) 
VALUES (%s,%s,%s,%s,%s)"""


def sku_normalization_answer(rd, status, hnhb):
    if rd in hnhb_list and hnhb == 1:
        return -1
    else:
        return 0 if pd.isna(status) or status == 0 else 1


def fast_growing_sku_answer(answer_new, fast_growing):
    return 1 if answer_new == 1 and fast_growing == 1 else 0


def baby_p_target_sku(mark, a1, a2, ts):
    return ts-3 if mark == 'P' and (a1/ts < a2/(ts-3)) else ts


def generate_sku_df(df1, df2, i):
    df = pd.merge(df1, store_info_df, how='left', on='addressIDnum', sort=False, copy=False)
    df = pd.merge(df, sku_info_df[sku_info_df['category'] == category[i]],
                  how='left', on='mark', sort=False, copy=False)
    df = pd.merge(df, df2, how='left', on=['rid', 'product_id'], sort=False, copy=False)
    df['answer_new'] = df.apply(lambda x: sku_normalization_answer(x.RD, x.status, x.hnhb), axis=1)
    df = df[df['answer_new'] != -1]
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
    with MySQLInstance(**mysql_db_bi_task, dict_result=True) as db:
        db.executemany(sql_insert_sku, [tuple(x) for x in df[['rid', 'product_id', 'product_name',
                                                              'answer_new', 'taskid']].values])
    return sku_df


def sku_verification_normalization_answer(username, i):
    if pd.isna(username) or username == '':
        return 0
    elif i == 4:
        return 1 if 'N' in username else 0
    elif i == 5:
        return 1 if 'Y' in username else 0
    elif i == 6:
        return 1 if 'H' in username else 0
    elif i == 7:
        return 1 if 'T' in username else 0
    else:
        return 1


def generate_verification_df(df1, df2, df3, i):
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
    verification_df = df3[df3['sku_verification_count'] == 1].groupby('rid')['sku_verification_count'].count()
    verification_df = df.join(verification_df)
    verification_df['sku_verification_count'].fillna(0, inplace=True)
    return verification_df


def multi_option_normalization_answer(answer, option_index):
    if pd.isna(answer):
        return 0
    else:
        return 1 if option_index in answer.split(',') else 0


def generate_multi_option_df(df1, df2, df3, i):
    if i == 0 or i == 1 or i == 2 or i == 5:
        df = pd.merge(df1, qnair_info_df[(qnair_info_df['criteria'] == 'multi_option') & (
                qnair_info_df['category'] == category[i])], how='left', on='taskid', sort=False, copy=False)
        df = pd.merge(df, df2, how='left', on=['rid', 'qid'], sort=False, copy=False)
        df = pd.merge(df, df3, how='left', on='qid', sort=False, copy=False)
        df['answer_new'] = df.apply(lambda x: multi_option_normalization_answer(x.answer, x.option_index), axis=1)
        multi_option_df = df.reindex(columns=['rid', 'option_name', 'answer_new'])
        multi_option_df = multi_option_df.set_index(['rid', 'option_name']).unstack()
        multi_option_df.columns = multi_option_df.columns.droplevel(0)
        if i == 5:
            del multi_option_df['样品展示_以上都没有']
            multi_option_df['样品展示_以上都没有'] = 1 - multi_option_df.apply(np.max, axis=1)
        else:
            del multi_option_df['联合陈列_以上都没有']
            multi_option_df['联合陈列_以上都没有'] = 1 - multi_option_df.apply(np.max, axis=1)
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


def to_one_wave_result(rd, sd, ad, svd, mod, year, i):
    number_df, check_number_df = generate_number_df(rd, ad, i)
    image_df = generate_image_df(rd, ad, year, i)
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


sql_insert_report_new = """INSERT INTO t_pg_report_hsm (rid,addressIDnum,`month`,category,actual_sku,target_sku,
pg_display,total_display,pg_shelf,total_shelf,actual_fast_growing_sku,fast_growing_sku_compliance,base_sku,taskid,
category_num,check_all) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

sql_insert_report_old = """INSERT INTO t_pg_report_hsm (rid,addressIDnum,`month`,category,actual_sku,target_sku,
pg_display,total_display,pg_shelf,total_shelf,actual_fast_growing_sku,fast_growing_sku_compliance,base_sku,taskid,
category_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""


def check_vs_pp_total_shelf(new, old):
    if np.isnan(old) or (new == 0 and old == 0):
        return 1
    elif new == 0 or old == 0:
        return 0
    else:
        return 1 if abs(new-old)/new <= 0.2 else 0


def check_vs_pp_pg_shelf(new, old):
    if np.isnan(old) or (new == 0 and old == 0):
        return 1
    elif new == 0 or old == 0:
        return 0
    else:
        return 1 if abs(new-old)/new <= 0.2 else 0


def check_vs_pp_pg_display(nt, npg, ot, opg):
    if np.isnan(ot) or (nt == 0 and ot == 0):
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
    if np.isnan(old) or (new == 0 and old == 0):
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


def to_two_wave_result(i):
    logger.info('Run PID (%s)...' % os.getpid())
    sku_df_new = query_data_frame(mysql_db_ppzck_task, sql_get_sku % (
        task_new[i], time_selection_new, status_not_in_new))
    answer_df_new = query_data_frame(mysql_db_ppzck_task, sql_get_answer % (
        task_new[i], time_selection_new, status_not_in_new))
    sku_verification_df_new = query_data_frame(mysql_db_ppzck_task, sql_get_sku_verification % (
        task_new[i], time_selection_new, status_not_in_new))
    sku_df_old = query_data_frame(mysql_db_ppzck_task, sql_get_sku % (
        task_old[i], time_selection_old, status_not_in_old))
    answer_df_old = query_data_frame(mysql_db_ppzck_task, sql_get_answer % (
        task_old[i], time_selection_old, status_not_in_old))
    sku_verification_df_old = query_data_frame(mysql_db_ppzck_task, sql_get_sku_verification % (
        task_old[i], time_selection_old, status_not_in_old))
    sku_df_new.drop_duplicates(subset=['rid', 'product_id'], inplace=True)
    answer_df_new.drop_duplicates(subset=['rid', 'qid'], inplace=True)
    sku_df_old.drop_duplicates(subset=['rid', 'product_id'], inplace=True)
    answer_df_old.drop_duplicates(subset=['rid', 'qid'], inplace=True)
    if i == 0 or i == 1 or i == 2:
        multi_option_df_new = query_data_frame(mysql_db_ppzck_task, sql_get_multi_option % ('联合陈列_', task_new[i]))
        multi_option_df_new.drop_duplicates(subset=['qid', 'option_index'], inplace=True)
        multi_option_df_old = query_data_frame(mysql_db_ppzck_task, sql_get_multi_option % ('联合陈列_', task_old[i]))
        multi_option_df_old.drop_duplicates(subset=['qid', 'option_index'], inplace=True)
    elif i == 5:
        multi_option_df_new = query_data_frame(mysql_db_ppzck_task, sql_get_multi_option % ('样品展示_', task_new[i]))
        multi_option_df_new.drop_duplicates(subset=['qid', 'option_index'], inplace=True)
        multi_option_df_old = query_data_frame(mysql_db_ppzck_task, sql_get_multi_option % ('样品展示_', task_old[i]))
        multi_option_df_old.drop_duplicates(subset=['qid', 'option_index'], inplace=True)
    else:
        multi_option_df_new = DataFrame()
        multi_option_df_old = DataFrame()
    new_df, check_number_new_df = to_one_wave_result(
        rid_df_list_new[i], sku_df_new, answer_df_new, sku_verification_df_new, multi_option_df_new, year_new[i], i)
    old_df, check_number_old_df = to_one_wave_result(
        rid_df_list_old[i], sku_df_old, answer_df_old, sku_verification_df_old, multi_option_df_old, year_old[i], i)
    new_df = pd.merge(new_df, store_new_df, how='left', on='addressIDnum')
    old_df = pd.merge(old_df, store_old_df, how='left', on='addressIDnum')
    new_df = pd.merge(new_df, old_df, how='left', on='addressIDnum', suffixes=('', '_old'))
    new_df.drop_duplicates(subset='rid', inplace=True)
    new_df['check_total_shelf'] = new_df.apply(
        lambda x: check_vs_pp_total_shelf(x.total_shelf, x.total_shelf_old), axis=1)
    new_df['check_pg_shelf'] = new_df.apply(
        lambda x: check_vs_pp_pg_shelf(x.pg_shelf, x.pg_shelf_old), axis=1)
    new_df['check_pg_display'] = new_df.apply(
        lambda x: check_vs_pp_pg_display(x.total_display, x.pg_display, x.total_display_old, x.pg_display_old), axis=1)
    new_df['check_pg_sku'] = new_df.apply(
        lambda x: check_vs_pp_sku(i, x.actual_sku, x.actual_sku_old), axis=1)
    new_df['check_all'] = new_df.apply(
        lambda x: check_all(x.check_recent, x.shelf_display_verification_1, x.shelf_display_verification_2,
                            x.sku_verification_count, x.check_total_shelf, x.check_pg_shelf,
                            x.check_pg_display, x.check_pg_sku), axis=1)
    report_new_df = new_df.reindex(columns=report_order[i])
    report_old_df = old_df.reindex(columns=report_order[i])
    checkpoint_new_df = new_df.reindex(columns=checkpoint_order[i])
    checkpoint_old_df = old_df.reindex(columns=checkpoint_order[i])
    report_new_df.to_csv(new_file[i], index=False, encoding='utf_8_sig')
    report_old_df.to_csv(old_file[i], index=False, encoding='utf_8_sig')
    checkpoint_new_df.to_csv(new_checkpoint_file[i], index=False, encoding='utf_8_sig')
    checkpoint_old_df.to_csv(old_checkpoint_file[i], index=False, encoding='utf_8_sig')
    check_number_new_df.to_csv(new_checkpoint_number_file[i], index=False, encoding='utf_8_sig')
    check_number_old_df.to_csv(old_checkpoint_number_file[i], index=False, encoding='utf_8_sig')
    subject = category[i] + datetime.now().strftime('%Y-%m-%d')
    contents = ['附件中为前后两月数据及需检查的数据', ]
    attachments = [new_file[i], old_file[i], new_checkpoint_file[i], old_checkpoint_file[i],
                   new_checkpoint_number_file[i], old_checkpoint_number_file[i]]
    with EmailSender(**email) as email_sender:
        email_sender.send_email(to=to, subject=subject, contents=contents, attachments=attachments)
    os.remove(new_file[i])
    os.remove(old_file[i])
    os.remove(new_checkpoint_file[i])
    os.remove(old_checkpoint_file[i])
    os.remove(new_checkpoint_number_file[i])
    os.remove(old_checkpoint_number_file[i])
    with MySQLInstance(**mysql_db_bi_task, dict_result=True) as db:
        db.executemany(sql_insert_report_new, [tuple(x) for x in new_df.reindex(columns=insert_table_order_new).values])
        db.executemany(sql_insert_report_old, [tuple(x) for x in old_df.reindex(columns=insert_table_order_old).values])


if __name__ == '__main__':
    logger.info('Parent process %s.' % os.getpid())
    p = Pool(4)
    for index in range(8):
        p.apply_async(to_two_wave_result, args=(index,))
    p.close()
    p.join()
    end_time = datetime.now()
    logger.info('time_consumed: %s' % (end_time-start_time))
