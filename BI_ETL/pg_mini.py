#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import importlib
import sys
import os
import pandas as pd
from pandas import DataFrame
from MySQLManager import MySQLInstance
import configparser
from datetime import datetime
import logging

importlib.reload(sys)  # python3写法, python2写法：reload(sys) sys.setdefaultencoding('utf8')

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
log_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'log_pg_mini.log')
f_handler = logging.FileHandler(log_file)
f_handler.setLevel(logging.INFO)
formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
f_handler.setFormatter(formatter)
logger.addHandler(f_handler)
s_handler = logging.StreamHandler()
s_handler.setLevel(logging.DEBUG)
logger.addHandler(s_handler)

conf = configparser.ConfigParser()
conf_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config_pg_mini.ini')
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

pg_mm_task = conf.get('pg_mini', 'pg_mm_task')
status_not_in = conf.get('pg_mini', 'status_not_in')
time_selection = conf.get('pg_mini', 'time_selection')
category = conf.get('pg_mini', 'category').split()
hnhb_list = conf.get('pg_mini', 'hnhb_list').split()
result_order = conf.get('pg_mini', 'result_order').split()
result_rd_order = conf.get('pg_mini', 'result_rd_order').split()
qindex_list = map(int, conf.get('pg_mini', 'qindex_str').split())
pic_name = conf.get('pg_mini', 'pic_name').split()
pic = dict(zip(qindex_list, pic_name))
insert_table_basic_info_list = conf.get('pg_mini', 'insert_table_basic_info').split()

start_time = datetime.now()
excel_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          ('pg_mini_report'+start_time.strftime('%Y-%m-%d')+'.xlsx'))
excel_rd_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             ('pg_mini_rd_report'+start_time.strftime('%Y-%m-%d')+'.xlsx'))

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
GROUP BY tr.Id,tispe.product_id""" % (pg_mm_task, time_selection, status_not_in)

sql_get_address = """SELECT * FROM lenzbi.t_pg_report_mm_address"""

sql_get_sku = """SELECT * FROM lenzbi.t_pg_report_mm_sku"""

sql_get_image_url = """SELECT ta.response_id rid, ta.qid, ta.image, tq.taskid, tq.qindex 
FROM t_answer ta LEFT JOIN t_question tq ON ta.qid = tq.Id 
LEFT JOIN t_response tr ON ta.response_id = tr.Id
WHERE tr.taskid_owner IN (%s) AND tq.type = 3 
%s
AND tr.`status` NOT IN (%s) 
ORDER BY tr.Id,tq.qindex""" % (pg_mm_task, time_selection, status_not_in)

sql_delete_report = """DELETE FROM t_pg_report_mm WHERE taskid IN (%s)""" % pg_mm_task

sql_delete_sku = """DELETE FROM t_pg_report_mm_sku_details WHERE taskid IN (%s)""" % pg_mm_task

sql_insert_sku = """INSERT INTO t_pg_report_mm_sku_details (rid,product_id,product_name,
is_exist,taskid) VALUES (%s,%s,%s,%s,%s)"""

sql_insert_basic_info = """INSERT INTO t_pg_report_mm (rid,SEQ,Region,Market,RD,Sold_to,Ship_to,Store_name,
Store_type,Province,City,City_tier,Store_address,FW_date,TTL_SKU_compliance,FG_SKU_compliance,TTL_SKU_Num_TTL,
TTL_SKU_Num_HC,TTL_SKU_Num_PCC,TTL_SKU_Num_FHC,TTL_SKU_Num_OC,TTL_SKU_Num_FEM,TTL_SKU_Num_BC,TTL_SKU_Num_SC,
TTL_SKU_Num_SHAVE,FG_SKU_Num_TTL,FG_SKU_Num_HC,FG_SKU_Num_PCC,FG_SKU_Num_FHC,FG_SKU_Num_OC,FG_SKU_Num_FEM,
FG_SKU_Num_BC,FG_SKU_Num_SC,FG_SKU_Num_SHAVE,图像识别,门头照,佐证无日化产品售卖,HC_pic_without_VS,VS_pic,PCC_pic,FHC_pic,
OC_pic,FEM_pic,BC_pic,SC_pic,SHAVE_pic,Display_pic,TTL_SKU_Target_TTL,TTL_SKU_Target_HC,TTL_SKU_Target_PCC,
TTL_SKU_Target_FHC,TTL_SKU_Target_OC,TTL_SKU_Target_FEM,TTL_SKU_Target_BC,TTL_SKU_Target_SC,TTL_SKU_Target_SHAVE,
FG_SKU_Target_TTL,FG_SKU_Target_HC,FG_SKU_Target_PCC,FG_SKU_Target_FHC,FG_SKU_Target_OC,FG_SKU_Target_FEM,
FG_SKU_Target_BC,FG_SKU_Target_SC,FG_SKU_Target_SHAVE,HC_compliance,PCC_compliance,FHC_compliance,OC_compliance,
FEM_compliance,BC_compliance,SC_compliance,SHAVE_compliance,taskid) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""


def get_sku_criteria(store_type, rd, lmm, smm, hnhb):
    if rd not in hnhb_list:
        if store_type == 'LMM':
            return lmm
        else:
            return smm
    else:
        if hnhb == 0:
            if store_type == 'LMM':
                return lmm
            else:
                return smm
        else:
            return 0


def get_sku_exist(sku_criteria, status):
    if sku_criteria == 1:
        return status
    else:
        return 0


def get_image_url(taskid, rid, qid, year, image):
    if len(image) > 0:
        return '=HYPERLINK("http://pc.ppznet.com/task_pc/images.jsp?year='+year+'&taskid='+taskid+'&responseid='+rid\
               + '&qid='+qid+'","图片")'
    else:
        return ''


def main():
    result = DataFrame()
    with MySQLInstance(**mysql_db_ppzck_task, dict_result=True) as db:
        main_data = db.query(sql_get_main_data)
    if main_data:
        main_data_df = DataFrame(main_data)
        with MySQLInstance(**mysql_db_ppzck_task, dict_result=True) as db:
            pg_mm_address_df = DataFrame(db.query(sql_get_address))
            pg_mm_sku_df = DataFrame(db.query(sql_get_sku))
        main_data_df = pd.merge(main_data_df, pg_mm_address_df, how='left', on='SEQ')
        main_data_df = pd.merge(main_data_df, pg_mm_sku_df, how='left', on='product_id')
        main_data_df['ttl_sku_criteria'] = main_data_df.apply(lambda x:
                                                              get_sku_criteria(x.Store_type,
                                                                               x.RD, x.LMM, x.SMM, x.HNHB), axis=1)
        main_data_df['fg_sku_criteria'] = main_data_df.apply(lambda x:
                                                             get_sku_criteria(x.Store_type, x.RD, x.LMM_FG,
                                                                              x.SMM_FG, x.HNHB), axis=1)
        main_data_df['ttl_sku_exist'] = main_data_df.apply(lambda x:
                                                           get_sku_exist(x.ttl_sku_criteria, x.status), axis=1)
        main_data_df['fg_sku_exist'] = main_data_df.apply(lambda x:
                                                          get_sku_exist(x.fg_sku_criteria, x.status), axis=1)
        main_data_df.loc[main_data_df['ttl_sku_criteria'] == 0, 'status'] = ''
        with MySQLInstance(**mysql_db_ppzck_task, dict_result=True) as db:
            image_data_df = DataFrame(db.query(sql_get_image_url))
        rid_series = main_data_df['rid'].drop_duplicates()
        for rid in rid_series.values:
            df = main_data_df[main_data_df['rid'] == rid]
            new_df = df[['product_name', 'status']].set_index('product_name').T
            new_df['rid'] = rid
            new_df['TTL_SKU_Num_TTL'] = df['ttl_sku_exist'].sum()
            new_df['TTL_SKU_Target_TTL'] = df['ttl_sku_criteria'].sum()
            new_df['FG_SKU_Num_TTL'] = df['fg_sku_exist'].sum()
            new_df['FG_SKU_Target_TTL'] = df['fg_sku_criteria'].sum()
            new_df['TTL_SKU_compliance'] = round(df['ttl_sku_exist'].sum()/df['ttl_sku_criteria'].sum(), 4)
            new_df['FG_SKU_compliance'] = round(df['fg_sku_exist'].sum()/df['fg_sku_criteria'].sum(), 4)
            for each in category:
                new_df['TTL_SKU_Num_' + each] = df.loc[df['Category'] == each, 'ttl_sku_exist'].sum()
                new_df['TTL_SKU_Target_' + each] = df.loc[df['Category'] == each, 'ttl_sku_criteria'].sum()
                new_df['FG_SKU_Num_' + each] = df.loc[df['Category'] == each, 'fg_sku_exist'].sum()
                new_df['FG_SKU_Target_' + each] = df.loc[df['Category'] == each, 'fg_sku_criteria'].sum()
                new_df[each + '_compliance'] = round(new_df['TTL_SKU_Num_' +
                                                            each]/new_df['TTL_SKU_Target_' + each], 4)
            new_df = pd.merge(new_df, df.drop_duplicates('rid'), how='left', on='rid')
            new_df['图像识别'] = '=HYPERLINK("http://pc.ppznet.com/task_pc//shenhe/aicorrect/' \
                             'images.jsp?responseid=' + rid + '&addressidnum=' + \
                             new_df['SEQ'].values[0] + '&iffenqu=1","图像识别")'
            new_df['Month'] = new_df['FW_date'].dt.strftime('%Y/%m')
            year = str(new_df['FW_date'].dt.year.values[0])
            image_df = image_data_df[image_data_df['rid'] == rid]
            image_df = image_df.copy()
            image_df['image_url'] = image_df.apply(lambda x:
                                                   get_image_url(x.taskid, x.rid, x.qid, year, x.image), axis=1)
            image_new_df = image_df[['qindex', 'image_url']].set_index('qindex').T
            image_new_df.rename(columns=pic, index={'image_url': 0}, inplace=True)
            new_df = new_df.join(image_new_df)
            result = result.append(new_df)
        result['FW_date'] = result['FW_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        # with MySQLInstance(**mysql_db_bi_task, dict_result=True) as bi_db:
        #     bi_db.execute(sql_delete_report)
        #     bi_db.execute(sql_delete_sku)
        #     bi_db.executemany(sql_insert_sku, [tuple(x) for x in main_data_df[['rid', 'product_id',
        #                                                                        'product_name',
        #                                                                        'status', 'taskid']].values])
        #     bi_db.executemany(sql_insert_basic_info, [tuple(x)
        #                                               for x in result[insert_table_basic_info_list].values])
        result.sort_values(by='FW_date')
        result_rd = result.reindex(columns=result_rd_order)
        result = result.reindex(columns=result_order)
        result_rd.to_excel(excel_rd_file, sheet_name="pg_mini_rd_report", index=False)
        result.to_excel(excel_file, sheet_name="pg_mini_report", index=False)
    else:
        logger.info('No result.')


if __name__ == '__main__':
    main()
    end_time = datetime.now()
    logger.info('time_consumed: %s' % (end_time-start_time))
