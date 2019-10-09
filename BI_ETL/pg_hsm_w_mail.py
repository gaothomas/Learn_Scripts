#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os
import configparser
import pandas as pd
from datetime import datetime
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
conf_file = os.path.join(os.path.dirname(os.path.realpath('__file__')), 'config_pg_hsm_w.ini')
conf.read(conf_file, encoding='utf-8')

email = {
    'user': conf.get('email', 'user'),
    'password': conf.get('email', 'password'),
    'host': conf.get('email', 'host'),
    'port': conf.getint('email', 'port'),
}

to = conf.get('email', 'to').split()
month = conf.get('pg_hsm', 'month')


def main():
    category = ['hair', 'pcc', 'laundry', 'oral', 'fem', 'baby', 'skin', 'br']
    report_path = os.path.join(os.path.dirname(os.path.realpath('__file__')), ('pg_hsm_report_' + month + '.xlsx'))
    writer = pd.ExcelWriter(report_path)
    report_file = []
    report_data = []
    report_data_number = []
    for i in range(8):
        report_file.append(os.path.join(os.path.dirname(os.path.realpath('__file__')),
                                        ('pg_hsm_report_' + category[i] + '_' + month + '.xlsx')))
        report_data.append(pd.read_excel(report_file[i], category[i]))
        for k in report_data[i].columns:
            if '_pic' in k:
                report_data[i][k] = report_data[i][k].map(lambda x: '' if pd.isna(x) else '=' + x)
        report_data[i].to_excel(writer, category[i], index=False)
        report_data_number.append(pd.read_excel(report_file[i], category[i] + '_number'))
        report_data_number[i].to_excel(writer, category[i] + '_number', index=False)
        os.remove(report_file[i])
    writer.close()
    subject = 'P&G_HSM_Report_' + month + '_' + datetime.now().strftime('%Y-%m-%d')
    contents = ['附件中为' + month + '全部数据及需检查的数据', ]
    attachments = [report_path]
    with EmailSender(**email) as email_sender:
        email_sender.send_email(to=to, subject=subject, contents=contents, attachments=attachments)


if __name__ == '__main__':
    main()
    end_time = datetime.now()
    logger.info('time_consumed: %s' % (end_time-start_time))
