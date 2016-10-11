#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'agustin ventura'

import sys

sys.path.insert(0, "..\\..")
import mysql.connector


class DataSource:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.__open_connection()

    def __open_connection(self):
        try:
            self.cnx = mysql.connector.connect(**self.config)
        except Exception, e:
            self.logger.error(
                "Error establishing database connection with configuration " + self.config + ": " + e.message)
            self.cnx = None

    def __del__(self):
        if self.cnx is not None:
            self.cnx.close()

    def get_connection(self):
        if self.cnx is None:
            self.__open_connection()
        return self.cnx

    def execute_and_commit(self, query, arguments):
        cursor = self.cnx.cursor()
        cursor.execute(query, arguments)
        self.cnx.commit()
        cursor.close()
        return cursor

    def execute(self, query, arguments):
        cursor = self.cnx.cursor()
        cursor.execute(query, arguments)
        # cursor.close()
        return cursor
