#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'agustin ventura'

import logging
import sys

sys.path.insert(0, "..\\..")
import mysql.connector


class DataSource:
    def __init__(self, config):
        self.config = config
        self.__connect()

    def __del__(self):
        logging.debug("DataSource destroyed")
        self.close_connection()

    def __connect(self):
        try:
            logging.debug("Opening database connection")
            self.cnx = mysql.connector.connect(**self.config)
        except Exception, e:
            logging.error(
                "Error establishing database connection with configuration " + self.config + ": " + e.message)
            self.cnx = None

    def __get_cursor(self):
        try:
            self.cnx.ping()
        except:
            self.__connect()
        return self.cnx.cursor()

    def open_connection(self):
        if self.cnx is None:
            self.__connect()
        return self.cnx

    def close_connection(self):
        if self.cnx is not None:
            logging.debug("Closing database connection")
            self.cnx.disconnect()

    def execute_and_commit(self, query, arguments):
        cursor = self.__get_cursor()
        cursor.execute(query, arguments)
        self.cnx.commit()
        cursor.close()

    def get_row(self, query, arguments):
        cursor = self.__get_cursor()
        cursor.execute(query, arguments)
        row = cursor.fetchone()
        cursor.close()
        return row

    def get_rows(self, query, arguments):
        cursor = self.__get_cursor()
        cursor.execute(query, arguments)
        rows = cursor.fetchall()
        cursor.close()
        return rows
