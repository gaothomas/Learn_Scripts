#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import logging
import os
import configparser
import pandas as pd
import numpy as np
from pandas import DataFrame
from datetime import datetime
from MySQLManager import MySQLInstance

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
log_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'log_pg_cs_1906.log')
f_handler = logging.FileHandler(log_file)
f_handler.setLevel(logging.INFO)
formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
f_handler.setFormatter(formatter)
logger.addHandler(f_handler)
s_handler = logging.StreamHandler()
s_handler.setLevel(logging.DEBUG)
logger.addHandler(s_handler)

conf = configparser.ConfigParser()
conf_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config_pg_cs_1906.ini')
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

pg_cs_task = conf.get('pg_cs', 'pg_cs_task')
status_not_in = conf.get('pg_cs', 'status_not_in')
time_selection = conf.get('pg_cs', 'time_selection')

category = conf.get('pg_cs', 'category').split()

s_sku_order = conf.get('pg_cs', 's_sku_order').split()
result_order = conf.get('pg_cs', 'result_order').split()
result_rd_order = conf.get('pg_cs', 'result_rd_order').split()

qindex_list = map(int, conf.get('pg_cs', 'qindex_str').split())
pic_name = conf.get('pg_cs', 'pic_name').split()
pic = dict(zip(qindex_list, pic_name))

start_time = datetime.now()
excel_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          ('pg_cs_report'+start_time.strftime('%Y-%m-%d')+'.xlsx'))
excel_rd_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             ('pg_cs_rd_report'+start_time.strftime('%Y-%m-%d')+'.xlsx'))

sql_get_main_data = """SELECT tr.Id rid, tr.end_time FW_date, tt.addressIDnum SEQ,
tr.taskid_owner taskid, tispe.product_id, tispe.`status`
FROM t_image_store_product_exist tispe
LEFT JOIN t_response tr ON tispe.response_id = tr.Id
LEFT JOIN t_tasklaunch tt ON tr.taskid = tt.taskid
LEFT JOIN t_answer ta ON ta.response_id = tr.Id
LEFT JOIN t_question tq ON tq.Id = ta.qid
WHERE tr.taskid_owner IN (%s)
%s
AND tr.`status` NOT IN (%s)
AND tq.qindex = '0' AND ta.answer = '0'
GROUP BY tr.Id,tispe.product_id"""

sql_get_address = """SELECT * FROM lenzbi.t_pg_report_cs_address"""

sql_get_sku = """SELECT * FROM lenzbi.t_pg_report_cs_sku"""

sql_get_image_url = """SELECT ta.response_id rid, ta.qid, ta.image, tq.taskid, tq.qindex
FROM t_answer ta LEFT JOIN t_question tq ON ta.qid = tq.Id
LEFT JOIN t_response tr ON ta.response_id = tr.Id
WHERE tr.taskid_owner IN (%s) AND tq.type = 3
%s
AND tr.`status` NOT IN (%s)
ORDER BY tr.Id,tq.qindex"""

sql_get_answer = """SELECT ta.response_id rid, ta.answer, tq.taskid, tq.qindex
FROM t_answer ta LEFT JOIN t_question tq ON ta.qid = tq.Id
LEFT JOIN t_response tr ON ta.response_id = tr.Id
WHERE tr.taskid_owner IN (%s) AND tq.qindex IN (14, 15, 17, 18, 19, 22, 23, 24, 25) 
%s
AND tr.`status` NOT IN (%s)
GROUP BY tr.Id,tq.qindex"""


def query_data_frame(db_dict, sql, result=True):
    with MySQLInstance(**db_dict, dict_result=result) as db:
        if db.query(sql):
            return DataFrame(db.query(sql))
        else:
            logger.info('No result.')
            sys.exit()


def get_sku_criteria(store_type, lbt, sbt):
    return lbt if store_type == 'LBT' else sbt


def get_sku_exist(sku_criteria, status):
    return 0 if sku_criteria == 0 else status


def get_image_url(taskid, rid, qid, year, image):
    return '' if image is None or image == '' else '=HYPERLINK("http://pc.ppznet.com/task_pc/images.jsp?year=' + year \
                                                   + '&taskid=' + taskid + '&responseid=' + rid + '&qid=' + qid \
                                                   + '","图片")'


def alternative_normalize_one(answer):
    if answer == '0':
        return 1
    else:
        return 0


def alternative_normalize_two(answer):
    if answer == '1':
        return 1
    else:
        return 0


def alternative_normalize_counter_a(a, b):
    if a == 0:
        return 0
    elif b == '0':
        return 1
    elif b == '2':
        return 2
    else:
        return 0


def alternative_normalize_counter_b(a, b):
    if a == 0:
        return 0
    elif b == '1':
        return 1
    else:
        return 0


def alternative_normalize_three(a, b):
    if a == 1 or a == 2:
        return b
    else:
        return 0


def alternative_normalize_skin_shelf(answer):
    if answer:
        return float(answer)
    else:
        return 0


def alternative_normalize_shelf_no_competitor(a, b):
    if a == 0:
        return 2
    elif b == '0':
        return 0
    else:
        return 1


def alternative_normalize_shelf_no_competitor_words(a, b):
    if a == 0:
        return '无货架'
    elif b == '0':
        return '有竞品'
    else:
        return '无竞品'


def alternative_normalize_highlighter_a(answer):
    if isinstance(answer, str):
        temp_list = answer.split(',')
        if '0' in temp_list:
            return 1
        else:
            return 0
    else:
        return 0


def alternative_normalize_highlighter_b(answer):
    if isinstance(answer, str):
        temp_list = answer.split(',')
        if '1' in temp_list:
            return 1
        else:
            return 0
    else:
        return 0


def alternative_normalize_highlighter_c(answer):
    if isinstance(answer, str):
        temp_list = answer.split(',')
        if '2' in temp_list:
            return 1
        else:
            return 0
    else:
        return 0


def qualify_counter(count_a_kv, count_b_kv, best_selling_area, no_competitor, sku_compliance):
    if count_a_kv == 1 and best_selling_area == 1 and no_competitor == 1 and sku_compliance >= 0.7:
        return 1
    elif count_a_kv == 2 and no_competitor == 1 and sku_compliance >= 0.7:
        return 2
    elif count_b_kv == 1 and best_selling_area == 1 and no_competitor == 1 and sku_compliance >= 0.7:
        return 3
    else:
        return 0


def qualify_counter_words(count_a_kv, count_b_kv, best_selling_area, no_competitor, sku_compliance):
    if count_a_kv == 1 and best_selling_area == 1 and no_competitor == 1 and sku_compliance >= 0.7:
        return '合格A Counter'
    elif count_a_kv == 2 and no_competitor == 1 and sku_compliance >= 0.7:
        return '合格Old A Counter'
    elif count_b_kv == 1 and best_selling_area == 1 and no_competitor == 1 and sku_compliance >= 0.7:
        return '合格 NB Counter'
    else:
        return '无合格Counter'


def main():
    result = DataFrame()
    main_data_df = query_data_frame(mysql_db_ppzck_task,
                                    sql_get_main_data % (pg_cs_task, time_selection, status_not_in))
    pg_mm_address_df = query_data_frame(mysql_db_ppzck_task, sql_get_address)
    pg_mm_sku_df = query_data_frame(mysql_db_ppzck_task, sql_get_sku)
    image_data_df = query_data_frame(mysql_db_ppzck_task,
                                     sql_get_image_url % (pg_cs_task, time_selection, status_not_in))
    answer_main_df = query_data_frame(mysql_db_ppzck_task,
                                      sql_get_answer % (pg_cs_task, time_selection, status_not_in))
    main_data_df = pd.merge(main_data_df, pg_mm_address_df, how='left', on='SEQ')
    main_data_df = pd.merge(main_data_df, pg_mm_sku_df, how='left', on='product_id')
    main_data_df = main_data_df[pd.notna(main_data_df.product_name)]
    main_data_df['ttl_sku_criteria'] = main_data_df.apply(lambda x:
                                                          get_sku_criteria(x.Store_type, x.LBT, x.SBT), axis=1)
    main_data_df['fg_sku_criteria'] = main_data_df.apply(lambda x:
                                                         get_sku_criteria(x.Store_type, x.LBT_FG, x.SBT_FG), axis=1)
    main_data_df['s_sku_criteria'] = main_data_df.apply(lambda x:
                                                        get_sku_criteria(x.Store_type, x.LBT_S, x.SBT_S), axis=1)
    main_data_df['ttl_sku_exist'] = main_data_df.apply(lambda x:
                                                       get_sku_exist(x.ttl_sku_criteria, x.status), axis=1)
    main_data_df['fg_sku_exist'] = main_data_df.apply(lambda x:
                                                      get_sku_exist(x.fg_sku_criteria, x.status), axis=1)
    main_data_df.loc[main_data_df['ttl_sku_criteria'] == 0, 'status'] = ''

    rid_series = main_data_df['rid'].drop_duplicates()
    for rid in rid_series.values:
        df = main_data_df[main_data_df['rid'] == rid].copy()
        temp_1 = max(df.loc[df['product_id'] == 8333, 'status'].values[0],
                     df.loc[df['product_id'] == 9301, 'status'].values[0])
        temp_2 = max(df.loc[df['product_id'] == 8327, 'status'].values[0],
                     df.loc[df['product_id'] == 9300, 'status'].values[0])
        df.loc[df['product_id'] == 8333, 'status'] = temp_1
        df.loc[df['product_id'] == 8333, 'ttl_sku_exist'] = temp_1
        df.loc[df['product_id'] == 8333, 'fg_sku_exist'] = temp_1
        df.loc[df['product_id'] == 8327, 'status'] = temp_2
        df.loc[df['product_id'] == 8327, 'ttl_sku_exist'] = temp_2
        df.loc[df['product_id'] == 8327, 'fg_sku_exist'] = temp_2

        counter_sku_df = df.loc[df['Category'] == 'SC_C',
                                ['product_name', 'ttl_sku_criteria',
                                 'fg_sku_criteria', 'ttl_sku_exist', 'fg_sku_exist']].copy()

        sku_df = df[['product_name', 'status']].set_index('product_name').T
        sku_df = sku_df.reindex(columns=s_sku_order)
        sku_df.rename(index={'status': 0}, inplace=True)

        pt_sum_df = pd.pivot_table(df, index=['Category', 'new_name'],
                                   values=['ttl_sku_exist', 'ttl_sku_criteria', 'fg_sku_exist', 'fg_sku_criteria'],
                                   aggfunc=np.sum)
        pt_max_df = pd.pivot_table(df, index=['Category', 'new_name'], values=['s_sku_criteria'], aggfunc=np.max)
        pt_df = pt_sum_df.join(pt_max_df)
        pt_df['ttl_s_sku_exist'] = pt_df.apply(lambda x: min(x.s_sku_criteria, x.ttl_sku_exist), axis=1)
        pt_df['fg_s_sku_exist'] = pt_df.apply(lambda x: min(x.s_sku_criteria, x.fg_sku_exist), axis=1)
        pt_df['ttl_s_sku_criteria'] = pt_df.apply(lambda x: min(x.s_sku_criteria, x.ttl_sku_criteria), axis=1)
        pt_df['fg_s_sku_criteria'] = pt_df.apply(lambda x: min(x.s_sku_criteria, x.fg_sku_criteria), axis=1)
        pt_df_new = pt_df.drop(index=['SC_B', 'SC_CB', 'SC_C']).reset_index()
        pt_df_new['status'] = pt_df_new['ttl_s_sku_exist']
        pt_df_new.loc[pt_df_new['ttl_s_sku_criteria'] == 0, 'status'] = ''

        new_df = pt_df_new[['new_name', 'status']].set_index('new_name').T
        new_df['rid'] = rid
        new_df['TTL_SKU_Num_TTL'] = pt_df_new['ttl_s_sku_exist'].sum()
        new_df['TTL_SKU_Target_TTL'] = pt_df_new['ttl_s_sku_criteria'].sum()
        new_df['FG_SKU_Num_TTL'] = pt_df_new['fg_s_sku_exist'].sum()
        new_df['FG_SKU_Target_TTL'] = pt_df_new['fg_s_sku_criteria'].sum()
        new_df['TTL_SKU_compliance'] = round(pt_df_new['ttl_s_sku_exist'].sum() /
                                             pt_df_new['ttl_s_sku_criteria'].sum(), 4)
        new_df['FG_SKU_compliance'] = round(pt_df_new['fg_s_sku_exist'].sum() /
                                            pt_df_new['fg_s_sku_criteria'].sum(), 4)
        for each in category:
            new_df['TTL_SKU_Num_' + each] = pt_df_new.loc[pt_df_new['Category'] == each, 'ttl_s_sku_exist'].sum()
            new_df['TTL_SKU_Target_' + each] = pt_df_new.loc[pt_df_new['Category'] == each,
                                                             'ttl_s_sku_criteria'].sum()
            new_df['FG_SKU_Num_' + each] = pt_df_new.loc[pt_df_new['Category'] == each, 'fg_s_sku_exist'].sum()
            new_df['FG_SKU_Target_' + each] = pt_df_new.loc[pt_df_new['Category'] == each,
                                                            'fg_s_sku_criteria'].sum()
            new_df[each + '_compliance'] = round(new_df['TTL_SKU_Num_' + each] /
                                                 new_df['TTL_SKU_Target_' + each], 4)
            new_df[each + '_fg_compliance'] = round(new_df['FG_SKU_Num_' + each] /
                                                    new_df['FG_SKU_Target_' + each], 4)
        new_df = pd.merge(new_df, df.drop_duplicates('rid'), how='left', on='rid')
        new_df['图像识别'] = '=HYPERLINK("http://pc.ppznet.com/task_pc//shenhe/aicorrect/' \
                         'images.jsp?responseid=' + rid + '&addressidnum=' + \
                         new_df['SEQ'].values[0] + '&iffenqu=1","图像识别")'
        new_df['Month'] = new_df['FW_date'].dt.strftime('%Y/%m')

        year = str(new_df['FW_date'].dt.year.values[0])
        image_df = image_data_df[image_data_df['rid'] == rid].copy()
        image_df['image_url'] = image_df.apply(lambda x:
                                               get_image_url(x.taskid, x.rid, x.qid, year, x.image), axis=1)
        image_new_df = image_df[['qindex', 'image_url']].set_index('qindex').T
        image_new_df.rename(columns=pic, index={'image_url': 0}, inplace=True)

        answer_df = answer_main_df[answer_main_df['rid'] == rid]
        answer_new_df = answer_df[['qindex', 'answer']].set_index('qindex').T
        answer_new_df = answer_new_df.reindex(columns=[14, 15, 17, 18, 19, 22, 23, 24, 25])
        answer_new_df.rename(index={'answer': 0}, inplace=True)
        answer_new_df['if_has_FHC_display'] = answer_new_df.apply(lambda x:
                                                                  alternative_normalize_one(x[14]), axis=1)
        answer_new_df['if_has_Counter'] = answer_new_df.apply(lambda x:
                                                              alternative_normalize_one(x[15]), axis=1)
        answer_new_df['best_selling_area'] = answer_new_df.apply(lambda x:
                                                                 alternative_normalize_one(x[18]), axis=1)
        answer_new_df['no_competitor'] = answer_new_df.apply(lambda x:
                                                             alternative_normalize_two(x[19]), axis=1)
        answer_new_df['Counter_A_kv'] = answer_new_df.apply(lambda x: alternative_normalize_counter_a(
            x['if_has_Counter'], x[17]), axis=1)
        answer_new_df['Counter_B_kv'] = answer_new_df.apply(lambda x: alternative_normalize_counter_b(
            x['if_has_Counter'], x[17]), axis=1)
        answer_new_df['Counter_A_best_selling_area'] = answer_new_df.apply(lambda x: alternative_normalize_three(
            x['Counter_A_kv'], x['best_selling_area']), axis=1)
        answer_new_df['Counter_A_no_competitor'] = answer_new_df.apply(lambda x: alternative_normalize_three(
            x['Counter_A_kv'], x['no_competitor']), axis=1)
        answer_new_df['Counter_B_best_selling_area'] = answer_new_df.apply(lambda x: alternative_normalize_three(
            x['Counter_B_kv'], x['best_selling_area']), axis=1)
        answer_new_df['Counter_B_no_competitor'] = answer_new_df.apply(lambda x: alternative_normalize_three(
            x['Counter_B_kv'], x['no_competitor']), axis=1)
        answer_new_df['Highlighter'] = answer_new_df.apply(lambda x:
                                                           alternative_normalize_one(x[22]), axis=1)
        answer_new_df['Skin_shelf'] = answer_new_df.apply(lambda x:
                                                          alternative_normalize_skin_shelf(x[24]), axis=1)
        answer_new_df['No_competitor'] = answer_new_df.apply(lambda x: alternative_normalize_shelf_no_competitor(
            x['Skin_shelf'], x[25]), axis=1)
        answer_new_df['No_competitor_words'] = answer_new_df.apply(
            lambda x: alternative_normalize_shelf_no_competitor_words(x['Skin_shelf'], x[25]), axis=1)
        answer_new_df['玉兰油新生塑颜金纯面霜'] = answer_new_df.apply(lambda x:
                                                           alternative_normalize_highlighter_a(x[23]), axis=1)
        answer_new_df['玉兰油水感透白亮肤面霜'] = answer_new_df.apply(lambda x:
                                                           alternative_normalize_highlighter_b(x[23]), axis=1)
        answer_new_df['玉兰油多效修护霜'] = answer_new_df.apply(lambda x:
                                                        alternative_normalize_highlighter_c(x[23]), axis=1)
        answer_new_df['Qualified_Highlighter'] = answer_new_df.apply(lambda x:
                                                                     min(x['Highlighter'], x['玉兰油新生塑颜金纯面霜'],
                                                                         x['玉兰油水感透白亮肤面霜'],
                                                                         x['玉兰油多效修护霜']), axis=1)
        if answer_new_df['if_has_Counter'].values[0] == 0:
            counter_sku_df['ttl_sku_exist'] = 0
            counter_sku_df['fg_sku_exist'] = 0
        answer_new_df['counter_sku_compliance'] = round(counter_sku_df['ttl_sku_exist'].sum() /
                                                        counter_sku_df['ttl_sku_criteria'].sum(), 4)
        answer_new_df['counter_fg_sku_compliance'] = round(counter_sku_df['fg_sku_exist'].sum() /
                                                           counter_sku_df['fg_sku_criteria'].sum(), 4)
        answer_new_df['Counter_A_sku_compliance'] = answer_new_df.apply(lambda x: alternative_normalize_three(
            x['Counter_A_kv'], x['counter_sku_compliance']), axis=1)
        answer_new_df['Counter_A_fg_sku_compliance'] = answer_new_df.apply(lambda x: alternative_normalize_three(
            x['Counter_A_kv'], x['counter_fg_sku_compliance']), axis=1)
        answer_new_df['Counter_B_sku_compliance'] = answer_new_df.apply(lambda x: alternative_normalize_three(
            x['Counter_B_kv'], x['counter_sku_compliance']), axis=1)
        answer_new_df['Counter_B_fg_sku_compliance'] = answer_new_df.apply(lambda x: alternative_normalize_three(
            x['Counter_B_kv'], x['counter_fg_sku_compliance']), axis=1)
        answer_new_df['Qualified_Counter'] = answer_new_df.apply(lambda x: qualify_counter(
            x['Counter_A_kv'], x['Counter_B_kv'], x['best_selling_area'], x['no_competitor'],
            x['counter_sku_compliance']), axis=1)
        answer_new_df['Qualified_Counter_words'] = answer_new_df.apply(lambda x: qualify_counter_words(
            x['Counter_A_kv'], x['Counter_B_kv'], x['best_selling_area'], x['no_competitor'],
            x['counter_sku_compliance']), axis=1)

        counter_sku_new_df = counter_sku_df[['product_name', 'ttl_sku_exist']].set_index('product_name').T
        counter_sku_new_df.rename(index={'ttl_sku_exist': 0}, inplace=True)

        new_df = new_df.join(image_new_df)
        new_df = new_df.join(sku_df)
        new_df = new_df.join(answer_new_df)
        new_df = new_df.join(counter_sku_new_df)
        result = result.append(new_df)

    result['FW_date'] = result['FW_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    result.sort_values(by='FW_date')
    result_rd = result.reindex(columns=result_rd_order)
    result = result.reindex(columns=result_order)
    result_rd.to_excel(excel_rd_file, sheet_name="pg_cs_rd_report", index=False)
    result.to_excel(excel_file, sheet_name="pg_cs_report", index=False)


if __name__ == '__main__':
    main()
    end_time = datetime.now()
    logger.info('time_consumed: %s' % (end_time-start_time))
