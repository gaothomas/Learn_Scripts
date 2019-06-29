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

# 生成日志文件
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

# 读取配置文件
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

category = conf.get('pg_hsm', 'category').split()
task_new = []
task_old = []
report_order = []
checkpoint_order = []
new_file = []
old_file = []
checkpoint_file = []

for each_category in category:
    task_new.append(conf.get('pg_hsm_' + each_category, 'task_new'))
    task_old.append(conf.get('pg_hsm_' + each_category, 'task_old'))
    report_order.append(conf.get('pg_hsm_' + each_category, 'report_order').split())
    checkpoint_order.append(conf.get('pg_hsm_' + each_category, 'checkpoint_order').split())
    # Excel文件路径
    new_file.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                 ('pg_hsm_report_new_' + each_category +
                                  datetime.now().strftime('%Y-%m-%d') + '.xlsx')))
    old_file.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                 ('pg_hsm_report_old_' + each_category +
                                  datetime.now().strftime('%Y-%m-%d') + '.xlsx')))
    checkpoint_file.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                        ('pg_hsm_report_checkpoint_' + each_category +
                                         datetime.now().strftime('%Y-%m-%d') + '.xlsx')))

insert_table_order_new = conf.get('pg_hsm', 'insert_table_order_new').split()
insert_table_order_old = conf.get('pg_hsm', 'insert_table_order_old').split()

# sql获取门店信息
sql_get_store_info = """SELECT addressIDnum, SEQ, Biz_Team, Division, Market, RD, Province, City, City_Level, 
Store_Name, Store_Type, New_Store_Type, Banner, Sub_banner, Address FROM lenzbi.t_pg_report_hsm_address"""

# sql获取门店列表
sql_get_store_list = """SELECT tt.addressIDnum, tr.id rid 
FROM t_response tr LEFT JOIN t_tasklaunch tt ON tr.taskid = tt.taskid 
WHERE tr.taskid_owner IN (%s) 
%s 
AND tr.`status` NOT IN (%s) 
GROUP BY tt.addressIDnum"""

# sql获取数字答案
sql_get_answer_number = """SELECT tr.id rid, ta.answer, tprhq.`month`, tprhq.category, tprhq.taskid, tprhq.qid, 
tprhq.qindex, tprhq.title, tprhq.criteria, tt.addressIDnum FROM t_response tr 
LEFT JOIN lenzbi.t_pg_report_hsm_qnair tprhq ON tr.taskid_owner = tprhq.taskid 
LEFT JOIN t_answer ta ON tr.id = ta.response_id AND tprhq.qid = ta.qid 
LEFT JOIN t_tasklaunch tt ON tr.taskid = tt.taskid 
WHERE tr.taskid_owner IN (%s) 
%s 
AND tr.`status` NOT IN (%s) 
AND tprhq.criteria NOT IN ('pic','multi_option','single_option','verification') 
AND tprhq.category = '%s'
GROUP BY tr.id,tprhq.qindex"""

# sql获取图片答案
sql_get_answer_image = """SELECT tr.id rid, ta.image, tprhq.qid, tprhq.title, tprhq.taskid FROM t_response tr 
LEFT JOIN lenzbi.t_pg_report_hsm_qnair tprhq ON tr.taskid_owner = tprhq.taskid 
LEFT JOIN t_answer ta ON tr.id = ta.response_id AND tprhq.qid = ta.qid 
WHERE tr.taskid_owner IN (%s) 
%s 
AND tr.`status` NOT IN (%s) 
AND tprhq.criteria IN ('pic') 
AND tprhq.category = '%s'
GROUP BY tr.id,tprhq.qindex"""

# sql获取多选题答案
sql_get_answer_multi_option = """SELECT tr.id rid, ta.answer, CONCAT('%s',tqo.option_value) option_name, 
tqo.option_index FROM t_response tr 
LEFT JOIN lenzbi.t_pg_report_hsm_qnair tprhq ON tr.taskid_owner = tprhq.taskid 
LEFT JOIN t_answer ta ON tr.id = ta.response_id AND tprhq.qid = ta.qid 
LEFT JOIN t_question_option tqo ON tprhq.qid = tqo.question_id
WHERE tr.taskid_owner IN (%s) 
%s
AND tr.`status` NOT IN (%s) 
AND tprhq.criteria IN ('multi_option') 
AND tprhq.category = '%s'
GROUP BY tr.id,tqo.option_index"""

# sql获取单选题答案
sql_get_answer_single_option = """SELECT tr.id rid, CASE WHEN ta.answer = '0' THEN '是' ELSE '否' END answer_option, 
tprhq.title FROM t_response tr 
LEFT JOIN lenzbi.t_pg_report_hsm_qnair tprhq ON tr.taskid_owner = tprhq.taskid 
LEFT JOIN t_answer ta ON tr.id = ta.response_id AND tprhq.qid = ta.qid 
WHERE tr.taskid_owner IN (%s) 
%s 
AND tr.`status` NOT IN (%s) 
AND tprhq.criteria IN ('single_option') 
AND tprhq.category = '%s'
GROUP BY tr.id,tprhq.qindex"""

# sql获取shelf&display审核人员与备注
sql_get_answer_verification = """SELECT tr.id rid, ta.answer,
tprhq.title FROM t_response tr
LEFT JOIN lenzbi.t_pg_report_hsm_qnair tprhq ON tr.taskid_owner = tprhq.taskid 
LEFT JOIN t_answer ta ON tr.id = ta.response_id AND tprhq.qid = ta.qid 
WHERE tr.taskid_owner IN (%s) 
%s 
AND tr.`status` NOT IN (%s) 
AND tprhq.criteria IN ('verification') 
AND tprhq.category = '%s'
GROUP BY tr.id,tprhq.qindex"""

# sql获取sku答案
sql_get_answer_sku = """SELECT tr.id rid, tr.taskid_owner taskid, tprhs.product_id, tprhs.product_name, tprha.RD, 
tispe.`status`, tprhs.fast_growing, tprhs.hnhb, tprhs.target_sku, tprhs.mark, 
tprhs.denominator_option1, tprhs.numerator_option1, tprhs.denominator_option2, tprhs.numerator_option2 
FROM t_response tr LEFT JOIN t_tasklaunch tt ON tr.taskid = tt.taskid
LEFT JOIN lenzbi.t_pg_report_hsm_sku tprhs ON LEFT(tt.addressIDnum, 1) = tprhs.mark 
LEFT JOIN lenzbi.t_pg_report_hsm_address tprha ON tt.addressIDnum = tprha.addressIDnum
LEFT JOIN t_image_store_product_exist tispe ON tr.Id = tispe.response_id AND tprhs.product_id = tispe.product_id
WHERE tr.taskid_owner IN (%s)
%s
AND tr.`status` NOT IN (%s) 
AND tprhs.category = '%s'
GROUP BY tr.id,tprhs.product_id"""


# sql获取sku审核人员与备注
sql_get_sku_verification = """SELECT tr.id rid, tre.username, tre.context, tre.examine_time 
FROM t_response tr LEFT JOIN t_response_examine tre ON tr.Id = tre.id 
WHERE tr.taskid_owner IN (%s) 
%s 
AND tr.`status` NOT IN (%s) 
ORDER BY tr.id, tre.examine_time"""

sql_delete_report = """DELETE FROM t_pg_report_hsm WHERE taskid IN (%s)"""

sql_delete_sku = """DELETE FROM t_pg_report_hsm_sku_details WHERE taskid IN (%s)"""

sql_insert_sku = """INSERT INTO t_pg_report_hsm_sku_details (rid,product_id,product_name,is_exist,taskid) 
VALUES (%s,%s,%s,%s,%s)"""

sql_insert_report_new = """INSERT INTO t_pg_report_hsm (rid,addressIDnum,`month`,category,actual_sku,target_sku,
pg_display,total_display,pg_shelf,total_shelf,actual_fast_growing_sku,fast_growing_sku_compliance,base_sku,taskid,
category_num,check_all) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

sql_insert_report_old = """INSERT INTO t_pg_report_hsm (rid,addressIDnum,`month`,category,actual_sku,target_sku,
pg_display,total_display,pg_shelf,total_shelf,actual_fast_growing_sku,fast_growing_sku_compliance,base_sku,taskid,
category_num) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""


# 从数据库中执行一次查询，将结果返回一个DataFrame
def query_data_frame(db_dict, sql, result=True):
    with MySQLInstance(**db_dict, dict_result=result) as db:
        if db.query(sql):
            return DataFrame(db.query(sql))
        else:
            logger.info('No result.')
            sys.exit()


# 判断字符串是否非负数
def is_non_negative_number(num):
    pattern = re.compile(r'^[1-9]\d*\.\d+$|0\.\d+$')
    result = pattern.match(num)
    return True if result else num.isdigit()


# 数字答案标准化,返回处理好的答案
def number_normalization_answer_new(criteria, answer):
    if answer is None or answer == '':
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


# 数字答案标准化,返回答案是否合规，1合规，0不合规
def number_normalization_answer_status(criteria, answer):
    if answer is None or answer == '':
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


# 生成图片链接
def get_image_url(taskid, rid, qid, year, image):
    return '' if image is None or image == '' else '=HYPERLINK("http://pc.ppznet.com/task_pc/images.jsp?year=' + year \
                                                   + '&taskid=' + taskid + '&responseid=' + rid + '&qid=' + qid \
                                                   + '","图片")'


# 多选题答案标准化
def multi_option_normalization_answer(answer, option_index):
    if answer is None:
        return 0
    else:
        return 1 if option_index in answer.split(',') else 0


# sku答案标准化
def sku_normalization_answer(rd, status, hnhb):
    if rd in hnhb_list and hnhb == 1:
        return -1
    else:
        return 0 if status is None or status == 0 else 1


# 生成fast_growing_sku答案
def fast_growing_sku_answer(answer_new, fast_growing):
    return 1 if answer_new == 1 and fast_growing == 1 else 0


# sku审核情况
def sku_verification(username, category_index):
    if len(username) == 0:
        return 0, 0
    elif len(username) == 1 and (username[0] is None or username[0] == ''):
        return 0, 0
    elif len(username) == 1:
        if category_index == 4:
            return 1 if 'N' in username[0] else 0, 0
        elif category_index == 5:
            return 1 if 'Y' in username[0] else 0, 0
        elif category_index == 6:
            return 1 if 'H' in username[0] else 0, 0
        elif category_index == 7:
            return 1 if 'T' in username[0] else 0, 0
        else:
            return 1, 0
    else:
        if category_index == 4:
            return 1 if 'N' in username[0] else 0, 1 if 'N' in username[1] else 0
        elif category_index == 5:
            return 1 if 'Y' in username[0] else 0, 1 if 'Y' in username[1] else 0
        elif category_index == 6:
            return 1 if 'H' in username[0] else 0, 1 if 'H' in username[1] else 0
        elif category_index == 7:
            return 1 if 'T' in username[0] else 0, 1 if 'T' in username[1] else 0
        else:
            return 0 if username[0] == '' else 1, 0 if username[1] == '' else 1


# 对比两期货架
def check_vs_pp_total_shelf(new, old):
    if np.isnan(old) or (new == 0 and old == 0):
        return 1
    elif new == 0 or old == 0:
        return 0
    else:
        return 1 if abs(new-old)/new <= 0.2 else 0


# 对比两期宝洁货架
def check_vs_pp_pg_shelf(new, old):
    if np.isnan(old) or (new == 0 and old == 0):
        return 1
    elif new == 0 or old == 0:
        return 0
    else:
        return 1 if abs(new-old)/new <= 0.2 else 0


# 对比两期宝洁特殊陈列
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


# 对比两期sku
def check_vs_pp_sku(category_index, new, old):
    if np.isnan(old) or (new == 0 and old == 0):
        return 1
    elif new == 0 or old == 0:
        return 0
    elif category_index == 0:
        return 1 if abs(new-old)/new <= 0.115 else 0
    elif category_index == 1:
        return 1 if abs(new-old)/new <= 0.13 else 0
    elif category_index == 2:
        return 1 if abs(new-old)/new <= 0.17 else 0
    elif category_index == 3:
        return 1 if abs(new-old)/new <= 0.14 else 0
    elif category_index == 4:
        return 1 if abs(new-old)/new <= 0.24 else 0
    elif category_index == 5:
        return 1 if abs(new-old)/new <= 0.30 else 0
    elif category_index == 6:
        return 1 if abs(new-old)/new <= 0.22 else 0
    else:
        return 1 if abs(new-old)/new <= 0.29 else 0


# 判断数据最终是否合规
def check_all(cr, sdv1, sdv2, sv1, sv2, cts, cps, cpd, cpsku):
    if sdv1 == 0 or sv1 == 0:
        return 0
    elif cr == 1 and ((cts + cps + cpd) == 3 or sdv2 == 1) and (cpsku == 1 or sv2 == 1):
        return 1
    else:
        return 0


# 生成单月结果集
def to_result(task, time, status_not, category_index):
    # 初始化空结果集
    result = DataFrame()
    # 获取数字答案
    answer_number_df = query_data_frame(mysql_db_ppzck_task,
                                        sql_get_answer_number % (task, time, status_not, category[category_index]))
    answer_number_df['answer_status'] = answer_number_df.apply(
        lambda x: number_normalization_answer_status(x.criteria, x.answer), axis=1)
    answer_number_df['answer_new'] = answer_number_df.apply(
        lambda x: number_normalization_answer_new(x.criteria, x.answer), axis=1)
    # 获取图片答案
    answer_image_df = query_data_frame(mysql_db_ppzck_task,
                                       sql_get_answer_image % (task, time, status_not, category[category_index]))
    # 获取sku答案
    answer_sku_df = query_data_frame(mysql_db_ppzck_task,
                                     sql_get_answer_sku % (task, time, status_not, category[category_index]))
    answer_sku_df['answer_new'] = answer_sku_df.apply(
        lambda x: sku_normalization_answer(x.RD, x.status, x.hnhb), axis=1)
    answer_sku_df = answer_sku_df[answer_sku_df['answer_new'] != -1]
    answer_sku_df['fast_growing_sku_exist'] = answer_sku_df.apply(
        lambda x: fast_growing_sku_answer(x.answer_new, x.fast_growing), axis=1)
    # 插入SKU数据
    with MySQLInstance(**mysql_db_bi_task, dict_result=True) as sku_db:
        sku_db.executemany(sql_insert_sku, [tuple(x) for x in answer_sku_df[['rid', 'product_id', 'product_name',
                                                                             'answer_new', 'taskid']].values])
    # 获取shelf&display审核人员与备注
    verification_shelf_display_df = query_data_frame(
        mysql_db_ppzck_task, sql_get_answer_verification % (task, time, status_not, category[category_index]))
    # 获取sku审核人员与备注
    verification_sku_df = query_data_frame(mysql_db_ppzck_task, sql_get_sku_verification % (task, time, status_not))
    # 获取rid
    rid_series = answer_number_df['rid'].drop_duplicates()
    # 获取多选题答案
    if category_index == 0 or category_index == 1 or category_index == 2:
        answer_multi_option_df = query_data_frame(
            mysql_db_ppzck_task,
            sql_get_answer_multi_option % ('联合陈列_', task, time, status_not, category[category_index]))
    elif category_index == 5:
        answer_multi_option_df = query_data_frame(
            mysql_db_ppzck_task,
            sql_get_answer_multi_option % ('样品展示_', task, time, status_not, category[category_index]))
    else:
        answer_multi_option_df = DataFrame()
    # 获取单选题答案
    if category_index == 7:
        answer_single_option_df = query_data_frame(
            mysql_db_ppzck_task, sql_get_answer_single_option % (task, time, status_not, category[category_index]))
    else:
        answer_single_option_df = DataFrame()

    for rid in rid_series.values:
        # 复制rid数字答案
        rid_answer_number_df = answer_number_df[answer_number_df['rid'] == rid]
        # 转置rid数字答案
        rid_answer_number_transpose_df = rid_answer_number_df[['title', 'answer_new']].set_index('title').T
        rid_answer_number_transpose_df.rename(index={'answer_new': 0}, inplace=True)
        rid_answer_number_transpose_df['rid'] = rid
        rid_answer_number_transpose_df['addressIDnum'] = rid_answer_number_df['addressIDnum'].values[0]
        rid_answer_number_transpose_df['month'] = rid_answer_number_df['month'].values[0]
        rid_answer_number_transpose_df['category'] = rid_answer_number_df['category'].values[0]
        rid_answer_number_transpose_df['taskid'] = rid_answer_number_df['taskid'].values[0]
        # 获取年份
        year = rid_answer_number_df['month'].values[0][:4]
        # 复制rid图片答案
        rid_answer_image_df = answer_image_df[answer_image_df['rid'] == rid].copy()
        rid_answer_image_df['image_url'] = \
            rid_answer_image_df.apply(lambda x: get_image_url(x.taskid, x.rid, x.qid, year, x.image), axis=1)
        # 转置rid图片答案
        rid_answer_image_transpose_df = rid_answer_image_df[['title', 'image_url']].set_index('title').T
        rid_answer_image_transpose_df.rename(index={'image_url': 0}, inplace=True)
        # 复制rid sku答案
        rid_answer_sku_df = answer_sku_df[answer_sku_df['rid'] == rid]
        # 转置rid sku答案
        rid_answer_sku_transpose_df = rid_answer_sku_df[['product_name', 'answer_new']].set_index('product_name').T
        rid_answer_sku_transpose_df.rename(index={'answer_new': 0}, inplace=True)
        rid_answer_sku_transpose_df['target_sku'] = rid_answer_sku_df['target_sku'].values[0]
        rid_answer_sku_transpose_df['actual_sku'] = rid_answer_sku_df['answer_new'].sum()
        rid_answer_sku_transpose_df['actual_fast_growing_sku'] = rid_answer_sku_df['fast_growing_sku_exist'].sum()
        rid_answer_sku_transpose_df['base_sku'] = (rid_answer_sku_transpose_df['actual_sku'].values[0] -
                                                   rid_answer_sku_transpose_df['actual_fast_growing_sku'].values[0])
        if category_index == 5:
            pt_option1_sum_df = pd.pivot_table(rid_answer_sku_df[rid_answer_sku_df['denominator_option1'] != 0],
                                               index=['denominator_option1'], values=['answer_new'], aggfunc=np.sum)
            pt_option1_max_df = pd.pivot_table(rid_answer_sku_df[rid_answer_sku_df['denominator_option1'] != 0],
                                               index=['denominator_option1'], values=['numerator_option1'],
                                               aggfunc=np.max)
            pt_option1_df = pt_option1_sum_df.join(pt_option1_max_df)
            pt_option1_df['actual_sku'] = pt_option1_df.apply(lambda x: min(x.answer_new, x.numerator_option1), axis=1)
            if rid_answer_sku_df['mark'].values[0] != 'H':
                pt_option2_sum_df = pd.pivot_table(rid_answer_sku_df[rid_answer_sku_df['denominator_option2'] != 0],
                                                   index=['denominator_option2'], values=['answer_new'], aggfunc=np.sum)
                pt_option2_max_df = pd.pivot_table(rid_answer_sku_df[rid_answer_sku_df['denominator_option2'] != 0],
                                                   index=['denominator_option2'], values=['numerator_option2'],
                                                   aggfunc=np.max)
                pt_option2_df = pt_option2_sum_df.join(pt_option2_max_df)
                pt_option2_df['actual_sku'] = pt_option2_df.apply(lambda x: min(x.answer_new,
                                                                                x.numerator_option2), axis=1)
                rid_answer_sku_transpose_df['actual_sku'] = max(pt_option1_df['actual_sku'].sum(),
                                                                pt_option2_df['actual_sku'].sum())
                if rid_answer_sku_df['mark'].values[0] == 'P' and \
                        (pt_option1_df['actual_sku'].sum()/rid_answer_sku_df['target_sku'].values[0]
                         < pt_option2_df['actual_sku'].sum()/(rid_answer_sku_df['target_sku'].values[0] - 3)):
                    '''rid_answer_sku_transpose_df['target_sku'] = 
                    rid_answer_sku_transpose_df['target_sku'].values[0] - 3'''
                    '''rid_answer_sku_transpose_df['target_sku'] = rid_answer_sku_transpose_df['target_sku'] - 3'''
                    rid_answer_sku_transpose_df['target_sku'] = np.subtract(rid_answer_sku_transpose_df[
                                                                                'target_sku'], 3)
            else:
                rid_answer_sku_transpose_df['actual_sku'] = pt_option1_df['actual_sku'].sum()
        rid_answer_sku_transpose_df['fast_growing_sku_compliance'] = \
            0 if rid_answer_sku_transpose_df['actual_sku'].values[0] == 0 else round(
                rid_answer_sku_transpose_df['actual_fast_growing_sku'].values[0] / rid_answer_sku_transpose_df[
                    'actual_sku'].values[0], 4)
        # 复制rid shelf&display审核信息
        rid_verification_shelf_display_df = verification_shelf_display_df[
            verification_shelf_display_df['rid'] == rid]
        # 复制rid display审核信息
        rid_verification_sku_df = verification_sku_df[verification_sku_df['rid'] == rid]
        # 转置并处理rid审核信息
        rid_verification_transpose_df = rid_verification_shelf_display_df[['title', 'answer']].set_index('title').T
        rid_verification_transpose_df.rename(index={'answer': 0}, inplace=True)
        rid_verification_transpose_df['shelf_display_verification_1'] = 0 \
            if rid_verification_transpose_df.iat[0, 2] is None or rid_verification_transpose_df.iat[0, 2] == '' else 1
        rid_verification_transpose_df['shelf_display_verification_2'] = 0 \
            if rid_verification_transpose_df.iat[0, 4] is None or rid_verification_transpose_df.iat[0, 4] == '' else 1
        rid_verification_transpose_df['sku_verification_1'] = sku_verification(
            rid_verification_sku_df['username'].values, category_index)[0]
        rid_verification_transpose_df['sku_verification_2'] = sku_verification(
            rid_verification_sku_df['username'].values, category_index)[1]

        if category_index == 0:
            # 主货架品牌分销
            for i in range(14):
                rid_answer_number_transpose_df[rid_answer_number_transpose_df.iloc[:, i+1].name + '_distribution'] = 1 \
                    if rid_answer_number_transpose_df.iat[0, i+1] > 0 else 0
            rid_answer_number_transpose_df['main_brand_no_shelf_distribution'] = \
                1 if rid_answer_number_transpose_df.iloc[0, 1:15].sum() == 0 else 0
            # 总货架组数
            rid_answer_number_transpose_df['total_shelf'] = rid_answer_number_transpose_df.iat[0, 0]
            # 宝洁货架组数
            rid_answer_number_transpose_df['pg_shelf'] = rid_answer_number_transpose_df.iloc[0, 1:6].sum()
            # 总特殊陈列数
            rid_answer_number_transpose_df['total_display'] = \
                rid_answer_number_transpose_df.iat[0, 15] + rid_answer_number_transpose_df.iat[0, 30] + \
                rid_answer_number_transpose_df.iat[0, 45] + rid_answer_number_transpose_df.iat[0, 60] + \
                rid_answer_number_transpose_df.iat[0, 75] + rid_answer_number_transpose_df.iat[0, 105] + \
                rid_answer_number_transpose_df.iat[0, 120]
            # 宝洁特殊陈列数
            rid_answer_number_transpose_df['pg_display'] = \
                rid_answer_number_transpose_df.iloc[0, 16:21].sum() + \
                rid_answer_number_transpose_df.iloc[0, 31:36].sum() + \
                rid_answer_number_transpose_df.iloc[0, 46:51].sum() + \
                rid_answer_number_transpose_df.iloc[0, 61:66].sum() + \
                rid_answer_number_transpose_df.iloc[0, 76:81].sum() + \
                rid_answer_number_transpose_df.iloc[0, 106:111].sum() + \
                rid_answer_number_transpose_df.iloc[0, 121:126].sum()
            # 特殊陈列种类
            rid_answer_number_transpose_df['non_equity_display_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 15] == 0 else 1
            rid_answer_number_transpose_df['equity_display_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 30] == 0 else 1
            rid_answer_number_transpose_df['endcap_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 45] == 0 else 1
            rid_answer_number_transpose_df['rack_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 60] == 0 else 1
            rid_answer_number_transpose_df['promotion_wall_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 75] == 0 else 1
            rid_answer_number_transpose_df['basket_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 105] == 0 else 1
            rid_answer_number_transpose_df['scenario_heap_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 120] == 0 else 1
            rid_answer_number_transpose_df['no_display_distribution'] = \
                1 if rid_answer_number_transpose_df['total_display'].values[0] == 0 else 0
            # 复制rid多选题答案
            rid_answer_multi_option_df = answer_multi_option_df[answer_multi_option_df['rid'] == rid].copy()
            rid_answer_multi_option_df['answer_new'] = rid_answer_multi_option_df.apply(
                lambda x: multi_option_normalization_answer(x.answer, x.option_index), axis=1)
            # 转置rid多选题答案
            rid_answer_multi_option_transpose_df = \
                rid_answer_multi_option_df[['option_name', 'answer_new']].set_index('option_name').T
            rid_answer_multi_option_transpose_df.rename(index={'answer_new': 0}, inplace=True)
            rid_answer_multi_option_transpose_df.iat[0, 14] = \
                1 if rid_answer_multi_option_transpose_df.iloc[0, 0:14].sum() == 0 else 0
            # 检验当期数据
            rid_answer_number_transpose_df['check_total_sum_shelf'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 1:15].sum() <= rid_answer_number_transpose_df.iat[0, 0] else 0
            rid_answer_number_transpose_df['check_total_sum_non_equity_display'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 16:30].sum() <= rid_answer_number_transpose_df.iat[0, 15] else 0
            rid_answer_number_transpose_df['check_total_sum_equity_display'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 31:45].sum() <= rid_answer_number_transpose_df.iat[0, 30] else 0
            rid_answer_number_transpose_df['check_total_sum_endcap'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 46:60].sum() <= rid_answer_number_transpose_df.iat[0, 45] else 0
            rid_answer_number_transpose_df['check_total_sum_rack'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 61:75].sum() <= rid_answer_number_transpose_df.iat[0, 60] else 0
            rid_answer_number_transpose_df['check_total_sum_promotion_wall'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 76:90].sum() <= rid_answer_number_transpose_df.iat[0, 75] else 0
            rid_answer_number_transpose_df['check_total_sum_packing_column'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 91:105].sum() <= rid_answer_number_transpose_df.iat[0, 90] else 0
            rid_answer_number_transpose_df['check_total_sum_basket'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 106:120].sum() <= rid_answer_number_transpose_df.iat[0,
                                                                                                            105] else 0
            rid_answer_number_transpose_df['check_total_sum_scenario_heap'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 121:135].sum() <= rid_answer_number_transpose_df.iat[0,
                                                                                                            120] else 0
            # 当期逻辑汇总
            rid_answer_number_transpose_df['check_recent'] = \
                1 if (rid_answer_number_transpose_df['check_total_sum_shelf'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_non_equity_display'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_equity_display'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_endcap'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_rack'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_promotion_wall'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_packing_column'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_basket'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_scenario_heap'].values[0]) == 9 else 0
            # 拼接生成rid结果集
            rid_answer_df = rid_answer_number_transpose_df.join(rid_answer_image_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_answer_sku_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_verification_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_answer_multi_option_transpose_df)
        elif category_index == 1:
            # 主货架品牌分销
            for i in range(9):
                rid_answer_number_transpose_df[
                    rid_answer_number_transpose_df.iloc[:, i+1].name.replace('_bath_lotion', '') + '_distribution'] = \
                    1 if (rid_answer_number_transpose_df.iat[0, i+1] + rid_answer_number_transpose_df.iat[0, i+11] +
                          rid_answer_number_transpose_df.iat[0, i+21]) > 0 else 0
            rid_answer_number_transpose_df['main_brand_no_shelf_distribution'] = \
                1 if (rid_answer_number_transpose_df.iloc[0, 1:10].sum() +
                      rid_answer_number_transpose_df.iloc[0, 11:20].sum() +
                      rid_answer_number_transpose_df.iloc[0, 21:30].sum()) == 0 else 0
            # 总货架组数
            rid_answer_number_transpose_df['total_shelf'] = (rid_answer_number_transpose_df.iat[0, 0] +
                                                             rid_answer_number_transpose_df.iat[0, 10] +
                                                             rid_answer_number_transpose_df.iat[0, 20])
            # 宝洁货架组数
            rid_answer_number_transpose_df['pg_shelf'] = (rid_answer_number_transpose_df.iat[0, 1] +
                                                          rid_answer_number_transpose_df.iat[0, 2] +
                                                          rid_answer_number_transpose_df.iat[0, 11] +
                                                          rid_answer_number_transpose_df.iat[0, 12] +
                                                          rid_answer_number_transpose_df.iat[0, 21] +
                                                          rid_answer_number_transpose_df.iat[0, 22])
            # 总特殊陈列数
            rid_answer_number_transpose_df['total_display'] = \
                rid_answer_number_transpose_df.iat[0, 30] + rid_answer_number_transpose_df.iat[0, 40] + \
                rid_answer_number_transpose_df.iat[0, 50] + rid_answer_number_transpose_df.iat[0, 60] + \
                rid_answer_number_transpose_df.iat[0, 70] + rid_answer_number_transpose_df.iat[0, 90] + \
                rid_answer_number_transpose_df.iat[0, 100]
            # 宝洁特殊陈列数
            rid_answer_number_transpose_df['pg_display'] = \
                rid_answer_number_transpose_df.iloc[0, 31:33].sum() + \
                rid_answer_number_transpose_df.iloc[0, 41:43].sum() + \
                rid_answer_number_transpose_df.iloc[0, 51:53].sum() + \
                rid_answer_number_transpose_df.iloc[0, 61:63].sum() + \
                rid_answer_number_transpose_df.iloc[0, 71:73].sum() + \
                rid_answer_number_transpose_df.iloc[0, 91:93].sum() + \
                rid_answer_number_transpose_df.iloc[0, 101:103].sum()
            # 特殊陈列种类
            rid_answer_number_transpose_df['non_equity_display_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 30] == 0 else 1
            rid_answer_number_transpose_df['equity_display_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 40] == 0 else 1
            rid_answer_number_transpose_df['endcap_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 50] == 0 else 1
            rid_answer_number_transpose_df['rack_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 60] == 0 else 1
            rid_answer_number_transpose_df['promotion_wall_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 70] == 0 else 1
            rid_answer_number_transpose_df['basket_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 90] == 0 else 1
            rid_answer_number_transpose_df['scenario_heap_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 100] == 0 else 1
            rid_answer_number_transpose_df['no_display_distribution'] = \
                1 if rid_answer_number_transpose_df['total_display'].values[0] == 0 else 0
            # 复制rid多选题答案
            rid_answer_multi_option_df = answer_multi_option_df[answer_multi_option_df['rid'] == rid].copy()
            rid_answer_multi_option_df['answer_new'] = rid_answer_multi_option_df.apply(
                lambda x: multi_option_normalization_answer(x.answer, x.option_index), axis=1)
            # 转置rid多选题答案
            rid_answer_multi_option_transpose_df = \
                rid_answer_multi_option_df[['option_name', 'answer_new']].set_index('option_name').T
            rid_answer_multi_option_transpose_df.rename(index={'answer_new': 0}, inplace=True)
            rid_answer_multi_option_transpose_df.iat[0, 9] = \
                1 if rid_answer_multi_option_transpose_df.iloc[0, 0:9].sum() == 0 else 0
            # 检验当期数据
            rid_answer_number_transpose_df['check_total_sum_bath_lotion_shelf'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 1:10].sum() <= rid_answer_number_transpose_df.iat[0, 0] else 0
            rid_answer_number_transpose_df['check_total_sum_soap_shelf'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 11:20].sum() <= rid_answer_number_transpose_df.iat[0, 10] else 0
            rid_answer_number_transpose_df['check_total_sum_hand_sanitizer_shelf'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 21:30].sum() <= rid_answer_number_transpose_df.iat[0, 20] else 0
            rid_answer_number_transpose_df['check_total_sum_non_equity_display'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 31:40].sum() <= rid_answer_number_transpose_df.iat[0, 30] else 0
            rid_answer_number_transpose_df['check_total_sum_equity_display'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 41:50].sum() <= rid_answer_number_transpose_df.iat[0, 40] else 0
            rid_answer_number_transpose_df['check_total_sum_endcap'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 51:60].sum() <= rid_answer_number_transpose_df.iat[0, 50] else 0
            rid_answer_number_transpose_df['check_total_sum_rack'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 61:70].sum() <= rid_answer_number_transpose_df.iat[0, 60] else 0
            rid_answer_number_transpose_df['check_total_sum_promotion_wall'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 71:80].sum() <= rid_answer_number_transpose_df.iat[0, 70] else 0
            rid_answer_number_transpose_df['check_total_sum_packing_column'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 81:90].sum() <= rid_answer_number_transpose_df.iat[0, 80] else 0
            rid_answer_number_transpose_df['check_total_sum_basket'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 91:100].sum() <= rid_answer_number_transpose_df.iat[0, 90] else 0
            rid_answer_number_transpose_df['check_total_sum_scenario_heap'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 101:110].sum() <= rid_answer_number_transpose_df.iat[0,
                                                                                                            100] else 0
            rid_answer_number_transpose_df['check_shelf_endcap'] = 0 if \
                (rid_answer_number_transpose_df.iat[0, 50] > 1
                 and min(rid_answer_number_transpose_df.iat[0, 0], rid_answer_number_transpose_df.iat[0, 10],
                         rid_answer_number_transpose_df.iat[0, 20]) == 0) else 1
            # 当期逻辑汇总
            rid_answer_number_transpose_df['check_recent'] = \
                1 if (rid_answer_number_transpose_df['check_total_sum_bath_lotion_shelf'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_soap_shelf'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_hand_sanitizer_shelf'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_non_equity_display'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_equity_display'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_endcap'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_rack'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_promotion_wall'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_packing_column'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_basket'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_scenario_heap'].values[0] +
                      rid_answer_number_transpose_df['check_shelf_endcap'].values[0]) == 12 else 0
            # 拼接生成rid结果集
            rid_answer_df = rid_answer_number_transpose_df.join(rid_answer_image_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_answer_sku_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_verification_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_answer_multi_option_transpose_df)
        elif category_index == 2:
            # 主货架品牌分销
            rid_answer_number_transpose_df[
                rid_answer_number_transpose_df.iloc[:, 51].name.replace('_rack', '') + '_distribution'] = \
                1 if (rid_answer_number_transpose_df.iat[0, 1] + rid_answer_number_transpose_df.iat[0, 8] +
                      rid_answer_number_transpose_df.iat[0, 17]) > 0 else 0
            rid_answer_number_transpose_df[
                rid_answer_number_transpose_df.iloc[:, 52].name.replace('_rack', '') + '_distribution'] = \
                1 if (rid_answer_number_transpose_df.iat[0, 10] + rid_answer_number_transpose_df.iat[0, 9] +
                      rid_answer_number_transpose_df.iat[0, 18]) > 0 else 0
            rid_answer_number_transpose_df[
                rid_answer_number_transpose_df.iloc[:, 53].name.replace('_rack', '') + '_distribution'] = \
                1 if rid_answer_number_transpose_df.iat[0, 10] > 0 else 0
            rid_answer_number_transpose_df[
                rid_answer_number_transpose_df.iloc[:, 54].name.replace('_rack', '') + '_distribution'] = \
                1 if rid_answer_number_transpose_df.iat[0, 11] > 0 else 0
            rid_answer_number_transpose_df[
                rid_answer_number_transpose_df.iloc[:, 55].name.replace('_rack', '') + '_distribution'] = \
                1 if (rid_answer_number_transpose_df.iat[0, 3] + rid_answer_number_transpose_df.iat[0, 12] +
                      rid_answer_number_transpose_df.iat[0, 19]) > 0 else 0
            rid_answer_number_transpose_df[
                rid_answer_number_transpose_df.iloc[:, 56].name.replace('_rack', '') + '_distribution'] = \
                1 if (rid_answer_number_transpose_df.iat[0, 4] + rid_answer_number_transpose_df.iat[0, 13] +
                      rid_answer_number_transpose_df.iat[0, 20]) > 0 else 0
            rid_answer_number_transpose_df[
                rid_answer_number_transpose_df.iloc[:, 57].name.replace('_rack', '') + '_distribution'] = \
                1 if (rid_answer_number_transpose_df.iat[0, 5] + rid_answer_number_transpose_df.iat[0, 14] +
                      rid_answer_number_transpose_df.iat[0, 21]) > 0 else 0
            rid_answer_number_transpose_df[
                rid_answer_number_transpose_df.iloc[:, 58].name.replace('_rack', '') + '_distribution'] = \
                1 if (rid_answer_number_transpose_df.iat[0, 6] + rid_answer_number_transpose_df.iat[0, 15] +
                      rid_answer_number_transpose_df.iat[0, 22]) > 0 else 0

            rid_answer_number_transpose_df['main_brand_no_shelf_distribution'] = \
                1 if (rid_answer_number_transpose_df.iloc[0, 1:7].sum() +
                      rid_answer_number_transpose_df.iloc[0, 8:16].sum() +
                      rid_answer_number_transpose_df.iloc[0, 17:23].sum()) == 0 else 0
            # 总货架组数
            rid_answer_number_transpose_df['total_shelf'] = (rid_answer_number_transpose_df.iat[0, 0] +
                                                             rid_answer_number_transpose_df.iat[0, 7] +
                                                             rid_answer_number_transpose_df.iat[0, 16])
            # 宝洁货架组数
            rid_answer_number_transpose_df['pg_shelf'] = (rid_answer_number_transpose_df.iloc[0, 1:3].sum() +
                                                          rid_answer_number_transpose_df.iloc[0, 8:11].sum() +
                                                          rid_answer_number_transpose_df.iloc[0, 17:19].sum())
            # 总特殊陈列数
            rid_answer_number_transpose_df['total_display'] = \
                rid_answer_number_transpose_df.iat[0, 23] + rid_answer_number_transpose_df.iat[0, 32] + \
                rid_answer_number_transpose_df.iat[0, 41] + rid_answer_number_transpose_df.iat[0, 50] + \
                rid_answer_number_transpose_df.iat[0, 59] + rid_answer_number_transpose_df.iat[0, 77] + \
                rid_answer_number_transpose_df.iat[0, 86]
            # 宝洁特殊陈列数
            rid_answer_number_transpose_df['pg_display'] = \
                rid_answer_number_transpose_df.iloc[0, 24:27].sum() + \
                rid_answer_number_transpose_df.iloc[0, 33:36].sum() + \
                rid_answer_number_transpose_df.iloc[0, 42:45].sum() + \
                rid_answer_number_transpose_df.iloc[0, 51:54].sum() + \
                rid_answer_number_transpose_df.iloc[0, 60:63].sum() + \
                rid_answer_number_transpose_df.iloc[0, 78:81].sum() + \
                rid_answer_number_transpose_df.iloc[0, 87:90].sum()
            # 特殊陈列种类
            rid_answer_number_transpose_df['non_equity_display_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 23] == 0 else 1
            rid_answer_number_transpose_df['equity_display_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 32] == 0 else 1
            rid_answer_number_transpose_df['endcap_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 41] == 0 else 1
            rid_answer_number_transpose_df['rack_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 50] == 0 else 1
            rid_answer_number_transpose_df['promotion_wall_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 59] == 0 else 1
            rid_answer_number_transpose_df['basket_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 77] == 0 else 1
            rid_answer_number_transpose_df['scenario_heap_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 86] == 0 else 1
            rid_answer_number_transpose_df['no_display_distribution'] = \
                1 if rid_answer_number_transpose_df['total_display'].values[0] == 0 else 0
            # 复制rid多选题答案
            rid_answer_multi_option_df = answer_multi_option_df[answer_multi_option_df['rid'] == rid].copy()
            rid_answer_multi_option_df['answer_new'] = rid_answer_multi_option_df.apply(
                lambda x: multi_option_normalization_answer(x.answer, x.option_index), axis=1)
            # 转置rid多选题答案
            rid_answer_multi_option_transpose_df = \
                rid_answer_multi_option_df[['option_name', 'answer_new']].set_index('option_name').T
            rid_answer_multi_option_transpose_df.rename(index={'answer_new': 0}, inplace=True)
            rid_answer_multi_option_transpose_df.iat[0, 7] = \
                1 if rid_answer_multi_option_transpose_df.iloc[0, 0:7].sum() == 0 else 0
            # 检验当期数据
            rid_answer_number_transpose_df['check_total_sum_powder_shelf'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 1:7].sum() <= rid_answer_number_transpose_df.iat[0, 0] else 0
            rid_answer_number_transpose_df['check_total_sum_liquid_shelf'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 8:16].sum() <= rid_answer_number_transpose_df.iat[0, 7] else 0
            rid_answer_number_transpose_df['check_total_sum_bar_shelf'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 17:23].sum() <= rid_answer_number_transpose_df.iat[0, 16] else 0
            rid_answer_number_transpose_df['check_total_sum_non_equity_display'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 24:32].sum() <= rid_answer_number_transpose_df.iat[0, 23] else 0
            rid_answer_number_transpose_df['check_total_sum_equity_display'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 33:41].sum() <= rid_answer_number_transpose_df.iat[0, 32] else 0
            rid_answer_number_transpose_df['check_total_sum_endcap'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 42:50].sum() <= rid_answer_number_transpose_df.iat[0, 41] else 0
            rid_answer_number_transpose_df['check_total_sum_rack'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 51:59].sum() <= rid_answer_number_transpose_df.iat[0, 50] else 0
            rid_answer_number_transpose_df['check_total_sum_promotion_wall'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 60:68].sum() <= rid_answer_number_transpose_df.iat[0, 59] else 0
            rid_answer_number_transpose_df['check_total_sum_packing_column'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 69:77].sum() <= rid_answer_number_transpose_df.iat[0, 68] else 0
            rid_answer_number_transpose_df['check_total_sum_basket'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 78:86].sum() <= rid_answer_number_transpose_df.iat[0, 77] else 0
            rid_answer_number_transpose_df['check_total_sum_scenario_heap'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 87:95].sum() <= rid_answer_number_transpose_df.iat[0, 86] else 0
            rid_answer_number_transpose_df['check_shelf_endcap'] = 0 if \
                (rid_answer_number_transpose_df.iat[0, 41] > 1
                 and min(rid_answer_number_transpose_df.iat[0, 0], rid_answer_number_transpose_df.iat[0, 7],
                         rid_answer_number_transpose_df.iat[0, 16]) == 0) else 1
            # 当期逻辑汇总
            rid_answer_number_transpose_df['check_recent'] = \
                1 if (rid_answer_number_transpose_df['check_total_sum_powder_shelf'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_liquid_shelf'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_bar_shelf'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_non_equity_display'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_equity_display'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_endcap'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_rack'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_promotion_wall'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_packing_column'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_basket'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_scenario_heap'].values[0] +
                      rid_answer_number_transpose_df['check_shelf_endcap'].values[0]) == 12 else 0
            # 拼接生成rid结果集
            rid_answer_df = rid_answer_number_transpose_df.join(rid_answer_image_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_answer_sku_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_verification_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_answer_multi_option_transpose_df)
        elif category_index == 3:
            # 主货架品牌分销
            for i in range(7):
                rid_answer_number_transpose_df[
                    rid_answer_number_transpose_df.iloc[:, i+1].name.replace('_toothpaste', '') + '_distribution'] = \
                    1 if (rid_answer_number_transpose_df.iat[0, i+1] +
                          rid_answer_number_transpose_df.iat[0, i+9]) > 0 else 0
            rid_answer_number_transpose_df['main_brand_no_shelf_distribution'] = \
                1 if (rid_answer_number_transpose_df.iloc[0, 1:8].sum() +
                      rid_answer_number_transpose_df.iloc[0, 9:16].sum()) == 0 else 0
            # 总货架组数
            rid_answer_number_transpose_df['total_shelf'] = (rid_answer_number_transpose_df.iat[0, 0] +
                                                             rid_answer_number_transpose_df.iat[0, 8])
            # 宝洁货架组数
            rid_answer_number_transpose_df['pg_shelf'] = (rid_answer_number_transpose_df.iat[0, 1] +
                                                          rid_answer_number_transpose_df.iat[0, 7] +
                                                          rid_answer_number_transpose_df.iat[0, 9] +
                                                          rid_answer_number_transpose_df.iat[0, 15])
            # 总特殊陈列数
            rid_answer_number_transpose_df['total_display'] = \
                rid_answer_number_transpose_df.iat[0, 16] + rid_answer_number_transpose_df.iat[0, 24] + \
                rid_answer_number_transpose_df.iat[0, 32] + rid_answer_number_transpose_df.iat[0, 40] + \
                rid_answer_number_transpose_df.iat[0, 48] + rid_answer_number_transpose_df.iat[0, 64] + \
                rid_answer_number_transpose_df.iat[0, 72]
            # 宝洁特殊陈列数
            rid_answer_number_transpose_df['pg_display'] = \
                rid_answer_number_transpose_df.iat[0, 17] + rid_answer_number_transpose_df.iat[0, 23] + \
                rid_answer_number_transpose_df.iat[0, 25] + rid_answer_number_transpose_df.iat[0, 31] + \
                rid_answer_number_transpose_df.iat[0, 33] + rid_answer_number_transpose_df.iat[0, 39] + \
                rid_answer_number_transpose_df.iat[0, 41] + rid_answer_number_transpose_df.iat[0, 47] + \
                rid_answer_number_transpose_df.iat[0, 49] + rid_answer_number_transpose_df.iat[0, 55] + \
                rid_answer_number_transpose_df.iat[0, 65] + rid_answer_number_transpose_df.iat[0, 71] + \
                rid_answer_number_transpose_df.iat[0, 73] + rid_answer_number_transpose_df.iat[0, 79]
            # 特殊陈列种类
            rid_answer_number_transpose_df['non_equity_display_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 16] == 0 else 1
            rid_answer_number_transpose_df['equity_display_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 24] == 0 else 1
            rid_answer_number_transpose_df['endcap_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 32] == 0 else 1
            rid_answer_number_transpose_df['rack_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 40] == 0 else 1
            rid_answer_number_transpose_df['promotion_wall_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 48] == 0 else 1
            rid_answer_number_transpose_df['basket_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 64] == 0 else 1
            rid_answer_number_transpose_df['scenario_heap_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 72] == 0 else 1
            rid_answer_number_transpose_df['no_display_distribution'] = \
                1 if rid_answer_number_transpose_df['total_display'].values[0] == 0 else 0
            # 检验当期数据
            rid_answer_number_transpose_df['check_total_sum_toothpaste_shelf'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 1:8].sum() <= rid_answer_number_transpose_df.iat[0, 0] else 0
            rid_answer_number_transpose_df['check_total_sum_toothbrush_shelf'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 9:16].sum() <= rid_answer_number_transpose_df.iat[0, 8] else 0
            rid_answer_number_transpose_df['check_total_sum_non_equity_display'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 17:24].sum() <= rid_answer_number_transpose_df.iat[0, 16] else 0
            rid_answer_number_transpose_df['check_total_sum_equity_display'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 25:32].sum() <= rid_answer_number_transpose_df.iat[0, 24] else 0
            rid_answer_number_transpose_df['check_total_sum_endcap'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 33:40].sum() <= rid_answer_number_transpose_df.iat[0, 32] else 0
            rid_answer_number_transpose_df['check_total_sum_rack'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 41:48].sum() <= rid_answer_number_transpose_df.iat[0, 40] else 0
            rid_answer_number_transpose_df['check_total_sum_promotion_wall'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 49:56].sum() <= rid_answer_number_transpose_df.iat[0, 48] else 0
            rid_answer_number_transpose_df['check_total_sum_packing_column'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 57:64].sum() <= rid_answer_number_transpose_df.iat[0, 56] else 0
            rid_answer_number_transpose_df['check_total_sum_basket'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 65:72].sum() <= rid_answer_number_transpose_df.iat[0, 64] else 0
            rid_answer_number_transpose_df['check_total_sum_scenario_heap'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 73:80].sum() <= rid_answer_number_transpose_df.iat[0, 72] else 0
            rid_answer_number_transpose_df['check_shelf_endcap'] = 0 if \
                (rid_answer_number_transpose_df.iat[0, 32] > 1 and
                 min(rid_answer_number_transpose_df.iat[0, 0], rid_answer_number_transpose_df.iat[0, 8]) == 0) else 1
            # 当期逻辑汇总
            rid_answer_number_transpose_df['check_recent'] = \
                1 if (rid_answer_number_transpose_df['check_total_sum_toothpaste_shelf'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_toothbrush_shelf'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_non_equity_display'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_equity_display'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_endcap'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_rack'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_promotion_wall'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_packing_column'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_basket'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_scenario_heap'].values[0] +
                      rid_answer_number_transpose_df['check_shelf_endcap'].values[0]) == 11 else 0
            # 拼接生成rid结果集
            rid_answer_df = rid_answer_number_transpose_df.join(rid_answer_image_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_answer_sku_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_verification_transpose_df)
        elif category_index == 4:
            # 主货架品牌分销
            for i in range(7):
                rid_answer_number_transpose_df[rid_answer_number_transpose_df.iloc[:, i+1].name + '_distribution'] = 1 \
                    if rid_answer_number_transpose_df.iat[0, i+1] > 0 else 0
            rid_answer_number_transpose_df['main_brand_no_shelf_distribution'] = \
                1 if rid_answer_number_transpose_df.iloc[0, 1:8].sum() == 0 else 0
            # 总货架组数
            rid_answer_number_transpose_df['total_shelf'] = rid_answer_number_transpose_df.iat[0, 0]
            # 宝洁货架组数
            rid_answer_number_transpose_df['pg_shelf'] = (rid_answer_number_transpose_df.iat[0, 1] +
                                                          rid_answer_number_transpose_df.iat[0, 2] +
                                                          rid_answer_number_transpose_df.iat[0, 7])
            # 总特殊陈列数
            rid_answer_number_transpose_df['total_display'] = \
                rid_answer_number_transpose_df.iat[0, 8] + rid_answer_number_transpose_df.iat[0, 16] + \
                rid_answer_number_transpose_df.iat[0, 24] + rid_answer_number_transpose_df.iat[0, 32] + \
                rid_answer_number_transpose_df.iat[0, 40] + rid_answer_number_transpose_df.iat[0, 56] + \
                rid_answer_number_transpose_df.iat[0, 64]
            # 宝洁特殊陈列数
            rid_answer_number_transpose_df['pg_display'] = \
                rid_answer_number_transpose_df.iloc[0, 9:11].sum() + rid_answer_number_transpose_df.iat[0, 15] + \
                rid_answer_number_transpose_df.iloc[0, 17:19].sum() + rid_answer_number_transpose_df.iat[0, 23] + \
                rid_answer_number_transpose_df.iloc[0, 25:27].sum() + rid_answer_number_transpose_df.iat[0, 31] + \
                rid_answer_number_transpose_df.iloc[0, 33:35].sum() + rid_answer_number_transpose_df.iat[0, 39] + \
                rid_answer_number_transpose_df.iloc[0, 41:43].sum() + rid_answer_number_transpose_df.iat[0, 47] + \
                rid_answer_number_transpose_df.iloc[0, 57:59].sum() + rid_answer_number_transpose_df.iat[0, 63] + \
                rid_answer_number_transpose_df.iloc[0, 65:67].sum() + rid_answer_number_transpose_df.iat[0, 71]
            # 特殊陈列种类
            rid_answer_number_transpose_df['non_equity_display_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 8] == 0 else 1
            rid_answer_number_transpose_df['equity_display_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 16] == 0 else 1
            rid_answer_number_transpose_df['endcap_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 24] == 0 else 1
            rid_answer_number_transpose_df['rack_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 32] == 0 else 1
            rid_answer_number_transpose_df['promotion_wall_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 40] == 0 else 1
            rid_answer_number_transpose_df['basket_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 56] == 0 else 1
            rid_answer_number_transpose_df['scenario_heap_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 64] == 0 else 1
            rid_answer_number_transpose_df['no_display_distribution'] = \
                1 if rid_answer_number_transpose_df['total_display'].values[0] == 0 else 0
            # 检验当期数据
            rid_answer_number_transpose_df['check_total_sum_shelf'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 1:8].sum() <= rid_answer_number_transpose_df.iat[0, 0] else 0
            rid_answer_number_transpose_df['check_total_sum_non_equity_display'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 9:16].sum() <= rid_answer_number_transpose_df.iat[0, 8] else 0
            rid_answer_number_transpose_df['check_total_sum_equity_display'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 17:24].sum() <= rid_answer_number_transpose_df.iat[0, 16] else 0
            rid_answer_number_transpose_df['check_total_sum_endcap'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 25:32].sum() <= rid_answer_number_transpose_df.iat[0, 24] else 0
            rid_answer_number_transpose_df['check_total_sum_rack'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 33:40].sum() <= rid_answer_number_transpose_df.iat[0, 32] else 0
            rid_answer_number_transpose_df['check_total_sum_promotion_wall'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 41:48].sum() <= rid_answer_number_transpose_df.iat[0, 40] else 0
            rid_answer_number_transpose_df['check_total_sum_packing_column'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 49:56].sum() <= rid_answer_number_transpose_df.iat[0, 48] else 0
            rid_answer_number_transpose_df['check_total_sum_basket'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 57:64].sum() <= rid_answer_number_transpose_df.iat[0, 56] else 0
            rid_answer_number_transpose_df['check_total_sum_scenario_heap'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 65:72].sum() <= rid_answer_number_transpose_df.iat[0, 64] else 0
            # 当期逻辑汇总
            rid_answer_number_transpose_df['check_recent'] = \
                1 if (rid_answer_number_transpose_df['check_total_sum_shelf'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_non_equity_display'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_equity_display'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_endcap'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_rack'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_promotion_wall'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_packing_column'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_basket'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_scenario_heap'].values[0]) == 9 else 0
            # 拼接生成rid结果集
            rid_answer_df = rid_answer_number_transpose_df.join(rid_answer_image_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_answer_sku_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_verification_transpose_df)
        elif category_index == 5:
            # 主货架品牌分销
            for i in range(7):
                rid_answer_number_transpose_df[rid_answer_number_transpose_df.iloc[:, i+1].name + '_distribution'] = 1 \
                    if rid_answer_number_transpose_df.iat[0, i+1] > 0 else 0
            rid_answer_number_transpose_df['main_brand_no_shelf_distribution'] = \
                1 if rid_answer_number_transpose_df.iloc[0, 1:8].sum() == 0 else 0
            # 总货架组数
            rid_answer_number_transpose_df['total_shelf'] = rid_answer_number_transpose_df.iat[0, 0]
            # 宝洁货架组数
            rid_answer_number_transpose_df['pg_shelf'] = rid_answer_number_transpose_df.iat[0, 1]
            # 总特殊陈列数
            rid_answer_number_transpose_df['total_display'] = \
                rid_answer_number_transpose_df.iat[0, 8] + rid_answer_number_transpose_df.iat[0, 16] + \
                rid_answer_number_transpose_df.iat[0, 24] + rid_answer_number_transpose_df.iat[0, 32] + \
                rid_answer_number_transpose_df.iat[0, 40] + rid_answer_number_transpose_df.iat[0, 56] + \
                rid_answer_number_transpose_df.iat[0, 64]
            # 宝洁特殊陈列数
            rid_answer_number_transpose_df['pg_display'] = \
                rid_answer_number_transpose_df.iat[0, 9] + rid_answer_number_transpose_df.iat[0, 17] + \
                rid_answer_number_transpose_df.iat[0, 25] + rid_answer_number_transpose_df.iat[0, 33] + \
                rid_answer_number_transpose_df.iat[0, 41] + rid_answer_number_transpose_df.iat[0, 57] + \
                rid_answer_number_transpose_df.iat[0, 65]
            # 特殊陈列种类
            rid_answer_number_transpose_df['non_equity_display_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 8] == 0 else 1
            rid_answer_number_transpose_df['equity_display_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 16] == 0 else 1
            rid_answer_number_transpose_df['endcap_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 24] == 0 else 1
            rid_answer_number_transpose_df['rack_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 32] == 0 else 1
            rid_answer_number_transpose_df['promotion_wall_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 40] == 0 else 1
            rid_answer_number_transpose_df['basket_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 56] == 0 else 1
            rid_answer_number_transpose_df['scenario_heap_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 64] == 0 else 1
            rid_answer_number_transpose_df['no_display_distribution'] = \
                1 if rid_answer_number_transpose_df['total_display'].values[0] == 0 else 0
            # 复制rid多选题答案
            rid_answer_multi_option_df = answer_multi_option_df[answer_multi_option_df['rid'] == rid].copy()
            rid_answer_multi_option_df['answer_new'] = rid_answer_multi_option_df.apply(
                lambda x: multi_option_normalization_answer(x.answer, x.option_index), axis=1)
            # 转置rid多选题答案
            rid_answer_multi_option_transpose_df = \
                rid_answer_multi_option_df[['option_name', 'answer_new']].set_index('option_name').T
            rid_answer_multi_option_transpose_df.rename(index={'answer_new': 0}, inplace=True)
            rid_answer_multi_option_transpose_df.iat[0, 7] = \
                1 if rid_answer_multi_option_transpose_df.iloc[0, 0:7].sum() == 0 else 0
            # 检验当期数据
            rid_answer_number_transpose_df['check_total_sum_shelf'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 1:8].sum() <= rid_answer_number_transpose_df.iat[0, 0] else 0
            rid_answer_number_transpose_df['check_total_sum_non_equity_display'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 9:16].sum() <= rid_answer_number_transpose_df.iat[0, 8] else 0
            rid_answer_number_transpose_df['check_total_sum_equity_display'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 17:24].sum() <= rid_answer_number_transpose_df.iat[0, 16] else 0
            rid_answer_number_transpose_df['check_total_sum_endcap'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 25:32].sum() <= rid_answer_number_transpose_df.iat[0, 24] else 0
            rid_answer_number_transpose_df['check_total_sum_rack'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 33:40].sum() <= rid_answer_number_transpose_df.iat[0, 32] else 0
            rid_answer_number_transpose_df['check_total_sum_promotion_wall'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 41:48].sum() <= rid_answer_number_transpose_df.iat[0, 40] else 0
            rid_answer_number_transpose_df['check_total_sum_packing_column'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 49:56].sum() <= rid_answer_number_transpose_df.iat[0, 48] else 0
            rid_answer_number_transpose_df['check_total_sum_basket'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 57:64].sum() <= rid_answer_number_transpose_df.iat[0, 56] else 0
            rid_answer_number_transpose_df['check_total_sum_scenario_heap'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 65:72].sum() <= rid_answer_number_transpose_df.iat[0, 64] else 0
            # 当期逻辑汇总
            rid_answer_number_transpose_df['check_recent'] = \
                1 if (rid_answer_number_transpose_df['check_total_sum_shelf'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_non_equity_display'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_equity_display'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_endcap'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_rack'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_promotion_wall'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_packing_column'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_basket'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_scenario_heap'].values[0]) == 9 else 0
            # 拼接生成rid结果集
            rid_answer_df = rid_answer_number_transpose_df.join(rid_answer_image_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_answer_sku_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_verification_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_answer_multi_option_transpose_df)
        elif category_index == 6:
            # 主货架品牌分销
            for i in range(4):
                rid_answer_number_transpose_df[rid_answer_number_transpose_df.iloc[:, i+1].name + '_distribution'] = 1 \
                    if rid_answer_number_transpose_df.iat[0, i+1] > 0 else 0
            rid_answer_number_transpose_df['main_brand_no_shelf_distribution'] = \
                1 if rid_answer_number_transpose_df.iloc[0, 1:5].sum() == 0 else 0
            # 总货架组数
            rid_answer_number_transpose_df['total_shelf'] = rid_answer_number_transpose_df.iat[0, 0]
            # 宝洁货架组数
            rid_answer_number_transpose_df['pg_shelf'] = rid_answer_number_transpose_df.iat[0, 1]
            # 总特殊陈列数
            rid_answer_number_transpose_df['total_display'] = \
                rid_answer_number_transpose_df.iat[0, 5] + rid_answer_number_transpose_df.iat[0, 10] + \
                rid_answer_number_transpose_df.iat[0, 15] + rid_answer_number_transpose_df.iat[0, 20] + \
                rid_answer_number_transpose_df.iat[0, 25] + rid_answer_number_transpose_df.iat[0, 35] + \
                rid_answer_number_transpose_df.iat[0, 40]
            # 宝洁特殊陈列数
            rid_answer_number_transpose_df['pg_display'] = \
                rid_answer_number_transpose_df.iat[0, 6] + rid_answer_number_transpose_df.iat[0, 11] + \
                rid_answer_number_transpose_df.iat[0, 16] + rid_answer_number_transpose_df.iat[0, 21] + \
                rid_answer_number_transpose_df.iat[0, 26] + rid_answer_number_transpose_df.iat[0, 36] + \
                rid_answer_number_transpose_df.iat[0, 41]
            # 特殊陈列种类
            rid_answer_number_transpose_df['non_equity_display_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 5] == 0 else 1
            rid_answer_number_transpose_df['equity_display_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 10] == 0 else 1
            rid_answer_number_transpose_df['endcap_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 15] == 0 else 1
            rid_answer_number_transpose_df['rack_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 20] == 0 else 1
            rid_answer_number_transpose_df['promotion_wall_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 25] == 0 else 1
            rid_answer_number_transpose_df['basket_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 35] == 0 else 1
            rid_answer_number_transpose_df['scenario_heap_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 40] == 0 else 1
            rid_answer_number_transpose_df['no_display_distribution'] = \
                1 if rid_answer_number_transpose_df['total_display'].values[0] == 0 else 0
            # 检验当期数据
            rid_answer_number_transpose_df['check_total_sum_shelf'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 1:5].sum() <= rid_answer_number_transpose_df.iat[0, 0] else 0
            rid_answer_number_transpose_df['check_total_sum_non_equity_display'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 6:10].sum() <= rid_answer_number_transpose_df.iat[0, 5] else 0
            rid_answer_number_transpose_df['check_total_sum_equity_display'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 11:15].sum() <= rid_answer_number_transpose_df.iat[0, 10] else 0
            rid_answer_number_transpose_df['check_total_sum_endcap'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 16:20].sum() <= rid_answer_number_transpose_df.iat[0, 15] else 0
            rid_answer_number_transpose_df['check_total_sum_rack'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 21:25].sum() <= rid_answer_number_transpose_df.iat[0, 20] else 0
            rid_answer_number_transpose_df['check_total_sum_promotion_wall'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 26:30].sum() <= rid_answer_number_transpose_df.iat[0, 25] else 0
            rid_answer_number_transpose_df['check_total_sum_packing_column'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 31:35].sum() <= rid_answer_number_transpose_df.iat[0, 30] else 0
            rid_answer_number_transpose_df['check_total_sum_basket'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 36:40].sum() <= rid_answer_number_transpose_df.iat[0, 35] else 0
            rid_answer_number_transpose_df['check_total_sum_scenario_heap'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 41:45].sum() <= rid_answer_number_transpose_df.iat[0, 40] else 0
            # 当期逻辑汇总
            rid_answer_number_transpose_df['check_recent'] = \
                1 if (rid_answer_number_transpose_df['check_total_sum_shelf'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_non_equity_display'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_equity_display'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_endcap'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_rack'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_promotion_wall'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_packing_column'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_basket'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_scenario_heap'].values[0]) == 9 else 0
            # 拼接生成rid结果集
            rid_answer_df = rid_answer_number_transpose_df.join(rid_answer_image_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_answer_sku_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_verification_transpose_df)
        elif category_index == 7:
            # 主货架品牌分销
            for i in range(4):
                rid_answer_number_transpose_df[rid_answer_number_transpose_df.iloc[:, i+1].name + '_distribution'] = 1 \
                    if rid_answer_number_transpose_df.iat[0, i+1] > 0 else 0
            rid_answer_number_transpose_df['main_brand_no_shelf_distribution'] = \
                1 if rid_answer_number_transpose_df.iloc[0, 1:5].sum() == 0 else 0
            # 总货架组数
            rid_answer_number_transpose_df['total_shelf'] = rid_answer_number_transpose_df.iat[0, 0]
            # 宝洁货架组数
            rid_answer_number_transpose_df['pg_shelf'] = rid_answer_number_transpose_df.iat[0, 1]
            # 总特殊陈列数
            rid_answer_number_transpose_df['total_display'] = \
                rid_answer_number_transpose_df.iat[0, 5] + rid_answer_number_transpose_df.iat[0, 10] + \
                rid_answer_number_transpose_df.iat[0, 15] + rid_answer_number_transpose_df.iat[0, 20] + \
                rid_answer_number_transpose_df.iat[0, 25] + rid_answer_number_transpose_df.iat[0, 30] + \
                rid_answer_number_transpose_df.iat[0, 35]
            # 宝洁特殊陈列数
            rid_answer_number_transpose_df['pg_display'] = \
                rid_answer_number_transpose_df.iat[0, 6] + rid_answer_number_transpose_df.iat[0, 11] + \
                rid_answer_number_transpose_df.iat[0, 16] + rid_answer_number_transpose_df.iat[0, 21] + \
                rid_answer_number_transpose_df.iat[0, 26] + rid_answer_number_transpose_df.iat[0, 31] + \
                rid_answer_number_transpose_df.iat[0, 36]
            # 特殊陈列种类
            rid_answer_number_transpose_df['non_equity_display_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 5] == 0 else 1
            rid_answer_number_transpose_df['equity_display_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 10] == 0 else 1
            rid_answer_number_transpose_df['endcap_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 15] == 0 else 1
            rid_answer_number_transpose_df['rack_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 20] == 0 else 1
            rid_answer_number_transpose_df['promotion_wall_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 25] == 0 else 1
            rid_answer_number_transpose_df['basket_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 30] == 0 else 1
            rid_answer_number_transpose_df['scenario_heap_distribution'] = \
                0 if rid_answer_number_transpose_df.iat[0, 35] == 0 else 1
            rid_answer_number_transpose_df['no_display_distribution'] = \
                1 if rid_answer_number_transpose_df['total_display'].values[0] == 0 else 0
            # 检验当期数据
            rid_answer_number_transpose_df['check_total_sum_shelf'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 1:5].sum() <= rid_answer_number_transpose_df.iat[0, 0] else 0
            rid_answer_number_transpose_df['check_total_sum_non_equity_display'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 6:10].sum() <= rid_answer_number_transpose_df.iat[0, 5] else 0
            rid_answer_number_transpose_df['check_total_sum_equity_display'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 11:15].sum() <= rid_answer_number_transpose_df.iat[0, 10] else 0
            rid_answer_number_transpose_df['check_total_sum_endcap'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 16:20].sum() <= rid_answer_number_transpose_df.iat[0, 15] else 0
            rid_answer_number_transpose_df['check_total_sum_rack'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 21:25].sum() <= rid_answer_number_transpose_df.iat[0, 20] else 0
            rid_answer_number_transpose_df['check_total_sum_promotion_wall'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 26:30].sum() <= rid_answer_number_transpose_df.iat[0, 25] else 0
            rid_answer_number_transpose_df['check_total_sum_basket'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 31:35].sum() <= rid_answer_number_transpose_df.iat[0, 30] else 0
            rid_answer_number_transpose_df['check_total_sum_scenario_heap'] = 1 if \
                rid_answer_number_transpose_df.iloc[0, 36:40].sum() <= rid_answer_number_transpose_df.iat[0, 35] else 0
            # 当期逻辑汇总
            rid_answer_number_transpose_df['check_recent'] = \
                1 if (rid_answer_number_transpose_df['check_total_sum_shelf'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_non_equity_display'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_equity_display'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_endcap'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_rack'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_promotion_wall'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_basket'].values[0] +
                      rid_answer_number_transpose_df['check_total_sum_scenario_heap'].values[0]) == 8 else 0
            # 复制rid单选题答案
            rid_answer_single_option_df = answer_single_option_df[answer_single_option_df['rid'] == rid]
            # 转置rid单选题答案
            rid_answer_single_option_transpose_df = \
                rid_answer_single_option_df[['title', 'answer_option']].set_index('title').T
            rid_answer_single_option_transpose_df.rename(index={'answer_option': 0}, inplace=True)
            # 拼接生成rid结果集
            rid_answer_df = rid_answer_number_transpose_df.join(rid_answer_image_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_answer_sku_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_verification_transpose_df)
            rid_answer_df = rid_answer_df.join(rid_answer_single_option_transpose_df)
            rid_answer_df['br_whole_store_distribution'] = 1 if (
                    rid_answer_df['total_shelf'].values[0] > 0 or rid_answer_df['total_display'].values[0] > 0 or
                    rid_answer_df['actual_sku'].values[0] > 0) else 0
            rid_answer_df['gillette_whole_store_distribution'] = 1 if (
                    rid_answer_df['pg_shelf'].values[0] > 0 or rid_answer_df['pg_display'].values[0] > 0 or
                    rid_answer_df['actual_sku'].values[0] > 0) else 0
        else:
            rid_answer_df = DataFrame()
        # 添加进初始结果集
        result = result.append(rid_answer_df, sort=False)
    return result, answer_number_df[answer_number_df['answer_status'] == 0]


# 生成两月对比报告
def to_report(category_index):
    logger.info('Run PID (%s)...' % os.getpid())
    new_df, check_number_new_df = to_result(task_new[category_index], time_selection_new, status_not_in_new,
                                            category_index)
    old_df, check_number_old_df = to_result(task_old[category_index], time_selection_old, status_not_in_old,
                                            category_index)
    store_new_df = query_data_frame(mysql_db_ppzck_task, sql_get_store_info)
    store_old_df = store_new_df.copy()
    for survey_index in range(8):
        store_new_df = pd.merge(store_new_df, query_data_frame(
            mysql_db_ppzck_task, sql_get_store_list % (task_new[survey_index], time_selection_new,
                                                       status_not_in_new)), how='left', on='addressIDnum')
        store_old_df = pd.merge(store_old_df, query_data_frame(
            mysql_db_ppzck_task, sql_get_store_list % (task_old[survey_index], time_selection_old,
                                                       status_not_in_old)), how='left', on='addressIDnum')
    store_new_df.iloc[:, 15:] = store_new_df.iloc[:, 15:].apply(pd.notna)
    store_new_df['category_num'] = store_new_df.iloc[:, 15:].apply(np.sum, axis=1)
    store_new_df.drop(columns=store_new_df.columns[15:23], inplace=True)
    store_old_df.iloc[:, 15:] = store_old_df.iloc[:, 15:].apply(pd.notna)
    store_old_df['category_num'] = store_old_df.iloc[:, 15:].apply(np.sum, axis=1)
    store_old_df.drop(columns=store_old_df.columns[15:23], inplace=True)

    new_df = pd.merge(new_df, store_new_df, how='left', on='addressIDnum')
    old_df = pd.merge(old_df, store_old_df, how='left', on='addressIDnum')

    new_df = pd.merge(new_df, old_df, how='left', on='addressIDnum', suffixes=('', '_old'))
    # 检验两期数据
    new_df['check_total_shelf'] = new_df.apply(
        lambda x: check_vs_pp_total_shelf(x.total_shelf, x.total_shelf_old), axis=1)
    new_df['check_pg_shelf'] = new_df.apply(
        lambda x: check_vs_pp_pg_shelf(x.pg_shelf, x.pg_shelf_old), axis=1)
    new_df['check_pg_display'] = new_df.apply(
        lambda x: check_vs_pp_pg_display(x.total_display, x.pg_display, x.total_display_old, x.pg_display_old), axis=1)
    new_df['check_pg_sku'] = new_df.apply(
        lambda x: check_vs_pp_sku(category_index, x.actual_sku, x.actual_sku_old), axis=1)
    new_df['check_all'] = new_df.apply(
        lambda x: check_all(x.check_recent, x.shelf_display_verification_1, x.shelf_display_verification_2,
                            x.sku_verification_1, x.sku_verification_2, x.check_total_shelf, x.check_pg_shelf,
                            x.check_pg_display, x.check_pg_sku), axis=1)

    report_new_df = new_df.reindex(columns=report_order[category_index])
    report_old_df = old_df.reindex(columns=report_order[category_index])

    checkpoint_new_df = new_df.reindex(columns=checkpoint_order[category_index])
    checkpoint_old_df = old_df.reindex(columns=checkpoint_order[category_index])

    report_new_df.to_excel(new_file[category_index], category[category_index], index=False)
    report_old_df.to_excel(old_file[category_index], category[category_index], index=False)

    writer_checkpoint = pd.ExcelWriter(checkpoint_file[category_index])
    checkpoint_new_df.to_excel(writer_checkpoint, category[category_index] + '_new', index=False)
    checkpoint_old_df.to_excel(writer_checkpoint, category[category_index] + '_old', index=False)
    check_number_new_df.to_excel(writer_checkpoint, category[category_index] + '_number_new', index=False)
    check_number_old_df.to_excel(writer_checkpoint, category[category_index] + '_number_old', index=False)
    writer_checkpoint.close()

    subject = category[category_index] + datetime.now().strftime('%Y-%m-%d')
    contents = ['附件中为前后两月数据及需检查的数据', ]
    attachments = [new_file[category_index], old_file[category_index], checkpoint_file[category_index]]
    with EmailSender(**email) as email_sender:
        email_sender.send_email(to=to, subject=subject, contents=contents, attachments=attachments)

    os.remove(new_file[category_index])
    os.remove(old_file[category_index])
    os.remove(checkpoint_file[category_index])

    # 写入数据库，注意NAN写不进mysql数据库
    with MySQLInstance(**mysql_db_bi_task, dict_result=True) as report_db:
        report_db.executemany(sql_insert_report_new,
                              [tuple(x) for x in new_df.reindex(columns=insert_table_order_new).values])
        report_db.executemany(sql_insert_report_old,
                              [tuple(x) for x in old_df.reindex(columns=insert_table_order_old).values])


if __name__ == '__main__':
    start_time = datetime.now()
    # 删除旧数据
    with MySQLInstance(**mysql_db_bi_task, dict_result=True) as delete_db:
        for task_index in range(8):
            delete_db.execute(sql_delete_report % task_new[task_index])
            delete_db.execute(sql_delete_sku % task_new[task_index])
            delete_db.execute(sql_delete_report % task_old[task_index])
            delete_db.execute(sql_delete_sku % task_old[task_index])
    logger.info('Parent process %s.' % os.getpid())
    # 开启进程池，一般CPU核数一半
    p = Pool(2)
    for index in range(8):
        p.apply_async(to_report, args=(index,))
    p.close()
    p.join()
    end_time = datetime.now()
    logger.info('time_consumed: %s' % (end_time-start_time))
