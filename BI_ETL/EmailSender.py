#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import yagmail
import logging


class EmailCon:
    def __init__(self, con, logger):
        self.con = con
        self.logger = logger

    def send_email(self, to, subject, contents, attachments):
        self.con.send(to=to, subject=subject, contents=contents, attachments=attachments)


class EmailSender:
    def __init__(self, user, password, host, port, logger=None):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        if logger is None:
            logger = logging.getLogger(__name__)
        self.logger = logger

    def __enter__(self):
        self.con = yagmail.SMTP(user=self.user, password=self.password, host=self.host, port=self.port)
        return EmailCon(self.con, self.logger)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.con.close()
