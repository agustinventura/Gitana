#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'agustin ventura'

import sys
sys.path.insert(0, "..\\..")

from github import Github
from github import NamedUser
from github import Repository
from github import Issue
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime
from extractor.init_db import config_db
import logging
import logging.handlers
import os
import glob

TYPE = "github"
URL = "https://github.com/gabrielecirulli/2048"
PRODUCT = "papyrus"
BEFORE_DATE = None
RECOVER_IMPORT = False
LOG_FOLDER = "logs"

class Issue2DbMain():

    def __init__(self, project_name, db_name, repo_name, type, url, product, before_date, recover_import):
        self.create_log_folder(LOG_FOLDER)
        LOG_FILENAME = LOG_FOLDER + "/issue2db_main"
        self.delete_previous_logs(LOG_FOLDER)
        self.logger = logging.getLogger(LOG_FILENAME)
        fileHandler = logging.FileHandler(LOG_FILENAME + "-" + db_name + ".log", mode='w')
        formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s", "%Y-%m-%d %H:%M:%S")

        fileHandler.setFormatter(formatter)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(fileHandler)

        self.type = type
        self.url = url
        self.project_name = project_name
        self.db_name = db_name
        self.repo_name = repo_name
        self.before_date = before_date
        self.recover_import = recover_import

        #self.querier = BugzillaQuerier(url, product, self.logger)

        self.cnx = mysql.connector.connect(**config_db.CONFIG)
        self.set_database()
        self.set_settings()

    def create_log_folder(self, name):
        if not os.path.exists(name):
            os.makedirs(name)

    def delete_previous_logs(self, path):
        files = glob.glob(path + "/*")
        for f in files:
            try:
                os.remove(f)
            except:
                continue

    def set_database(self):
        cursor = self.cnx.cursor()
        use_database = "USE " + self.db_name
        cursor.execute(use_database)
        cursor.close()

    def set_settings(self):
        cursor = self.cnx.cursor()
        cursor.execute("set global innodb_file_format = BARRACUDA")
        cursor.execute("set global innodb_file_format_max = BARRACUDA")
        cursor.execute("set global innodb_large_prefix = ON")
        cursor.execute("set global character_set_server = utf8")
        cursor.close()

    def extract(self):
        g = Github()
        users = g.search_users("gabrielecirulli")
        for user in users:
            repo = user.get_repo("2048")
            issues = repo.get_issues()
            print issues
            for issue in issues:
                print issue
        return


def main():
    a = Issue2DbMain(config_db.PROJECT_NAME, config_db.DB_NAME, config_db.REPO_NAME, TYPE, URL, PRODUCT, BEFORE_DATE, RECOVER_IMPORT)
    a.extract()

if __name__ == "__main__":
    main()