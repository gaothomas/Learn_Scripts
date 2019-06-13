#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import importlib
import sys
import logging
import os
import configparser
import requests
import re
import time
from MySQLManager import MySQLInstance
from datetime import datetime
from multiprocessing import Pool

# python3写法, python2写法：reload(sys) sys.setdefaultencoding('utf8')
importlib.reload(sys)

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
log_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'log.log')
f_handler = logging.FileHandler(log_file)
f_handler.setLevel(logging.INFO)
formatter = logging.Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
f_handler.setFormatter(formatter)
logger.addHandler(f_handler)
s_handler = logging.StreamHandler()
s_handler.setLevel(logging.DEBUG)
logger.addHandler(s_handler)

conf = configparser.ConfigParser()
conf_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config_database.ini')
conf.read(conf_file, encoding='utf-8')

mysql_db_ppzck_task = {
    'host': conf.get('ppzck_task', 'host'),
    'port': conf.getint('ppzck_task', 'port'),
    'username': conf.get('ppzck_task', 'username'),
    'password': conf.get('ppzck_task', 'password'),
    'schema': conf.get('ppzck_task', 'schema')
}

time_dict = {0: ('2019-01-01', '2019-04-01'),
             1: ('2019-04-01', '2019-04-10'),
             2: ('2019-04-10', '2019-04-20'),
             3: ('2019-04-20', '2019-06-20')}

sql_get_url = """SELECT tt.taskname, ttl.province, CONCAT(ttl.reallyAddress,ttl.addressIDnum) store, 
CASE WHEN tq.qindex=3 THEN '门头照' ELSE '体验台或柜台' END titlename, 
CONCAT('http://app.ppznet.com/2019/',tt.id,'/',tr.id,'/',tq.Id,'/') link, 
ta.image media FROM t_answer ta 
LEFT JOIN t_response tr ON ta.response_id = tr.Id 
LEFT JOIN t_question tq ON ta.qid = tq.Id 
LEFT JOIN t_tasklaunch ttl ON tr.taskid = ttl.taskid
LEFT JOIN t_task tt ON tr.taskid_owner = tt.id
WHERE tr.taskid_owner = 'bd4613b2feef444bbdb225edee23f4b6'
AND tr.`status` NOT IN ('2','5')
AND tq.qindex IN (3,11)
AND ta.image != ''
AND tr.end_time BETWEEN '%s' AND '%s'
UNION

SELECT tt.taskname, ttl.province, CONCAT(ttl.reallyAddress,ttl.addressIDnum) store, 
'门头照' titlename, 
CONCAT('http://app.ppznet.com/2019/',tt.id,'/',tr.id,'/',tq.Id,'/') link, 
ta.image media FROM t_answer ta 
LEFT JOIN t_response tr ON ta.response_id = tr.Id 
LEFT JOIN t_question tq ON ta.qid = tq.Id 
LEFT JOIN t_tasklaunch ttl ON tr.taskid = ttl.taskid
LEFT JOIN t_task tt ON tr.taskid_owner = tt.id
WHERE tr.taskid_owner = '49c485b25b5a49889477a79cbc6fb513'
AND tr.`status` NOT IN ('2','5')
AND tq.qindex IN (3)
AND ta.image != ''
AND tr.end_time BETWEEN '%s' AND '%s'
UNION

SELECT tt.taskname, ttl.province, CONCAT(ttl.reallyAddress,ttl.addressIDnum) store, 
CASE tq.qindex WHEN 3 THEN '门头照' 
WHEN 6 THEN '门口物料全景照'
WHEN 7 THEN 'vivo专区全景照'
WHEN 8 THEN 'vivo X27体验台全景照'
WHEN 9 THEN 'vivo X27柜台内照片'
WHEN 10 THEN 'vivo S1体验台全景照'
WHEN 11 THEN 'vivo S1柜台内照片' 
ELSE '拍摄店内除以上区域外，其他所有宣传物料'
END titlename, 
CONCAT('http://app.ppznet.com/2019/',tt.id,'/',tr.id,'/',tq.Id,'/') link, 
ta.image media FROM t_answer ta 
LEFT JOIN t_response tr ON ta.response_id = tr.Id 
LEFT JOIN t_question tq ON ta.qid = tq.Id 
LEFT JOIN t_tasklaunch ttl ON tr.taskid = ttl.taskid
LEFT JOIN t_task tt ON tr.taskid_owner = tt.id
WHERE tr.taskid_owner = '72b72b0af21b4d3f8580e2fe7f363e47'
AND tr.`status` NOT IN ('2','5')
AND tq.qindex IN (3,6,7,8,9,10,11,12)
AND ta.image != ''
AND tr.end_time BETWEEN '%s' AND '%s'
UNION

SELECT tt.taskname, ttl.province, CONCAT(ttl.reallyAddress,ttl.addressIDnum) store, 
CASE WHEN tq.qindex=3 THEN '门头照' ELSE '人员形象' END titlename, 
CONCAT('http://app.ppznet.com/2019/',tt.id,'/',tr.id,'/',tq.Id,'/') link, 
ta.image media FROM t_answer ta 
LEFT JOIN t_response tr ON ta.response_id = tr.Id 
LEFT JOIN t_question tq ON ta.qid = tq.Id 
LEFT JOIN t_tasklaunch ttl ON tr.taskid = ttl.taskid
LEFT JOIN t_task tt ON tr.taskid_owner = tt.id
WHERE tr.taskid_owner = '72b72b0af21b4d3f8580e2fe7f363e47'
AND tr.id IN (SELECT tr.id FROM t_response tr LEFT JOIN t_answer ta ON tr.id = ta.response_id 
LEFT JOIN t_question tq ON ta.qid = tq.Id 
WHERE tq.qindex = 63 AND ta.answer = '0' AND tr.taskid_owner ='72b72b0af21b4d3f8580e2fe7f363e47' 
AND tr.`status` NOT IN (2, 5) AND tr.percent > 0.2)
AND tr.`status` NOT IN ('2','5')
AND tq.qindex IN (3,13)
AND ta.image != ''
AND tr.end_time BETWEEN '%s' AND '%s'
UNION

SELECT tt.taskname, ttl.province, CONCAT(ttl.reallyAddress,ttl.addressIDnum) store, 
'培训落地' titlename, 
CONCAT('http://media.ppznet.com/audio/',tt.id,'/',tr.id,'/',tq.Id,'/') link, 
ta.answer media FROM t_answer ta 
LEFT JOIN t_response tr ON ta.response_id = tr.Id 
LEFT JOIN t_question tq ON ta.qid = tq.Id 
LEFT JOIN t_tasklaunch ttl ON tr.taskid = ttl.taskid
LEFT JOIN t_task tt ON tr.taskid_owner = tt.id
WHERE tr.taskid_owner = 'bd4613b2feef444bbdb225edee23f4b6'
AND tr.`status` NOT IN ('2','5')
AND tq.qindex IN (8)
AND ta.answer != ''
AND tr.end_time BETWEEN '%s' AND '%s'
UNION

SELECT tt.taskname, ttl.province, CONCAT(ttl.reallyAddress,ttl.addressIDnum) store,
'砍价录音' titlename, 
CONCAT('http://media.ppznet.com/audio/',tt.id,'/',tr.id,'/',tq.Id,'/') link, 
ta.answer media FROM t_answer ta 
LEFT JOIN t_response tr ON ta.response_id = tr.Id 
LEFT JOIN t_question tq ON ta.qid = tq.Id 
LEFT JOIN t_tasklaunch ttl ON tr.taskid = ttl.taskid
LEFT JOIN t_task tt ON tr.taskid_owner = tt.id
WHERE tr.taskid_owner = '49c485b25b5a49889477a79cbc6fb513'
AND tr.`status` NOT IN ('2','5')
AND tq.qindex IN (6)
AND ta.answer != ''
AND tr.end_time BETWEEN '%s' AND '%s'"""


def string_normalize(string):
    return re.sub('[!@#$<>*]', '', string)


def main(index):
    logger.info('Run PID %s (%s)...' % (index, os.getpid()))
    with MySQLInstance(**mysql_db_ppzck_task, dict_result=False) as db:
        tuple_url_data = db.query(sql_get_url % (time_dict[index] * 6))
    for tup in tuple_url_data:
        for md in tup[5].split(';'):
            file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                     'media_download', string_normalize(tup[0]),
                                     string_normalize(tup[1]), string_normalize(tup[2]), string_normalize(tup[3]))
            if not os.path.exists(file_path):
                os.makedirs(file_path)
            url = tup[4]+md
            response = requests.get(url)
            med = response.content
            with open(os.path.join(file_path, md.replace(':', '_')), 'wb') as f:
                f.write(med)
    time.sleep(1)


if __name__ == '__main__':
    start_time = datetime.now()
    logger.info('Parent process %s.' % os.getpid())
    p = Pool(4)
    for i in range(4):
        p.apply_async(main, args=(i,))
    p.close()
    p.join()
    end_time = datetime.now()
    logger.info('time_consumed: %s' % (end_time-start_time))
