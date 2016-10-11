#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'agustin ventura'

from data_source import DataSource
from extractor.util.db_util import DbUtil


class GithubDAO:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.data_source = DataSource(config, logger)
        self.db_util = DbUtil()

    def get_repo_id(self, project_name, repo_name):
        project_id = self.db_util.select_project_id(self.data_source.get_connection(), project_name, self.logger)
        repo_id = self.db_util.select_repo_id(self.data_source.get_connection(), project_id, repo_name, self.logger)
        return repo_id

    def get_issue_tracker_id(self, repo_id, url, type):
        query = "INSERT IGNORE INTO issue_tracker " \
                "VALUES (%s, %s, %s, %s)"
        arguments = [None, repo_id, url, type]
        cursor = self.data_source.execute_and_commit(query, arguments)
        repo_id = cursor.lastrowid
        if repo_id == 0:
            query = "SELECT id " \
                    "FROM issue_tracker " \
                    "WHERE url = %s"
            arguments = [url]
            cursor = self.data_source.execute(query, arguments)
            row = cursor.fetchone()
            if row:
                repo_id = row[0]
            else:
                self.logger("no issue tracker linked to " + str(url))
            cursor.close()
        return repo_id
