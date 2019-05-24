#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import importlib
import sys
import os
import pandas as pd
from pandas import Series, DataFrame
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

mysql_db_task = {
    'host': conf.get('task', 'host'),
    'port': conf.getint('task', 'port'),
    'username': conf.get('task', 'username'),
    'password': conf.get('task', 'password'),
    'schema': conf.get('task', 'schema')
}
pg_mm_task = conf.get('pg_mini', 'pg_mm_task')
status_not_in = conf.get('pg_mini', 'status_not_in')
time_selection = conf.get('pg_mini', 'time_selection')
category = conf.get('pg_mini', 'category').split()
hnhb_list = conf.get('pg_mini', 'hnhb_list').split()
result_order = conf.get('pg_mini', 'result_order').split()
qindex_list = map(int, conf.get('pg_mini', 'qindex_str').split())
pic_name = conf.get('pg_mini', 'pic_name').split()
pic = dict(zip(qindex_list, pic_name))

result = DataFrame()
start_time = datetime.now()


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


if __name__ == '__main__':
    with MySQLInstance(**mysql_db_task, dict_result=True) as db:
        rid_df = DataFrame(db.query("""SELECT tr.Id rid, tr.end_time FW_date, tt.addressIDnum SEQ,
        tr.taskid_owner taskid FROM t_response tr LEFT JOIN t_tasklaunch tt ON tr.taskid = tt.taskid
        LEFT JOIN t_answer ta ON ta.response_id = tr.Id
        LEFT JOIN t_question tq ON tq.Id = ta.qid
        WHERE tr.taskid_owner IN (%s)   
        %s  
        AND tr.`status` NOT IN (%s)
        AND tq.qindex = '0' AND ta.answer = '0'""" % (pg_mm_task, time_selection, status_not_in)))
        pg_mm_sku_df = DataFrame(db.query("""SELECT * FROM lenzbi.t_pg_report_mm_sku"""))
        pg_mm_address_df = DataFrame(db.query("""SELECT * FROM lenzbi.t_pg_report_mm_address"""))
        for rid in rid_df['rid'].values:
            logger.info('rid: %s is completed' % rid)
            con = db.query("""SELECT response_id rid,product_id,`status` 
            FROM t_image_store_product_exist WHERE response_id IN ('%s') 
            GROUP BY response_id,product_id""" % rid)
            if con:
                df = DataFrame(con)
                df = pd.merge(df, rid_df, how='left', on='rid')
                df = pd.merge(df, pg_mm_address_df[['SEQ', 'RD', 'Store_type']], how='left', on='SEQ')
                df = pd.merge(df, pg_mm_sku_df, how='left', on='product_id')
                df['ttl_sku_criteria'] = df.apply(lambda x:
                                                  get_sku_criteria(x.Store_type, x.RD, x.LMM, x.SMM, x.HNHB), axis=1)
                df['fg_sku_criteria'] = df.apply(lambda x: get_sku_criteria(x.Store_type, x.RD, x.LMM_FG, x.SMM_FG,
                                                                            x.HNHB), axis=1)
                df['ttl_sku_exist'] = df.apply(lambda x:
                                               get_sku_exist(x.ttl_sku_criteria, x.status), axis=1)
                df['fg_sku_exist'] = df.apply(lambda x:
                                              get_sku_exist(x.fg_sku_criteria, x.status), axis=1)
                df.loc[df['ttl_sku_criteria'] == 0, 'status'] = ''
                new_df = df[['product_name', 'status']].set_index('product_name').T
                new_df['rid'] = rid
                new_df['TTL_SKU_Num_TTL'] = df['ttl_sku_exist'].sum()
                new_df['TTL_SKU_Target_TTL'] = df['ttl_sku_criteria'].sum()
                new_df['FG_SKU_Num_TTL'] = df['fg_sku_exist'].sum()
                new_df['FG_SKU_Target_TTL'] = df['fg_sku_criteria'].sum()
                new_df['TTL_SKU_compliance'] = df['ttl_sku_exist'].sum() / df['ttl_sku_criteria'].sum()
                new_df['FG_SKU_compliance'] = df['fg_sku_exist'].sum() / df['fg_sku_criteria'].sum()
                for each in category:
                    new_df['TTL_SKU_Num_' + each] = df.loc[df['Category'] == each, 'ttl_sku_exist'].sum()
                    new_df['TTL_SKU_Target_' + each] = df.loc[df['Category'] == each, 'ttl_sku_criteria'].sum()
                    new_df['FG_SKU_Num_' + each] = df.loc[df['Category'] == each, 'fg_sku_exist'].sum()
                    new_df['FG_SKU_Target_' + each] = df.loc[df['Category'] == each, 'fg_sku_criteria'].sum()
                new_df = pd.merge(new_df, rid_df, how='left', on='rid')
                new_df = pd.merge(new_df, pg_mm_address_df, how='left', on='SEQ')
                new_df['图像识别'] = '=HYPERLINK("http://pc.ppznet.com/task_pc//shenhe/aicorrect/images.jsp?responseid='\
                                 + rid+'&addressidnum='+new_df['SEQ'].values[0]+'&iffenqu=1","图像识别")'
                year = str(new_df['FW_date'].dt.year.values[0])
                image_df = DataFrame(db.query("""SELECT ta.response_id rid, ta.qid, ta.image, tq.taskid, tq.qindex 
                FROM t_answer ta LEFT JOIN t_question tq ON ta.qid = tq.Id 
                WHERE response_id IN ('%s') AND tq.type = 3 
                ORDER BY tq.qindex""" % rid))
                image_df['image_url'] = image_df.apply(lambda x:
                                                       get_image_url(x.taskid, x.rid, x.qid, year, x.image), axis=1)
                image_df = image_df[['qindex', 'image_url']].set_index('qindex').T
                image_df.rename(columns=pic, index={'image_url': 0}, inplace=True)
                new_df = new_df.join(image_df)
                result = result.append(new_df)
    result = result.reindex(columns=result_order)
    result.to_excel('pg_mini_report'+start_time.strftime('%Y-%m-%d')+'.xlsx', sheet_name="pg_mini_report", index=False)
    end_time = datetime.now()
    logger.info('time_consumed: %s' % (end_time-start_time))
