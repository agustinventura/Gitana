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
        self.__insert_issue_tracker(repo_id, type, url)
        repo_id = self.__select_issue_tracker_id(repo_id, url)
        return repo_id

    def insert_issue(self, own_id, summary, version, created_at, updated_at):
        query = "INSERT IGNORE INTO issue(id, own_id, summary, version, created_at, last_change_at) " \
                "VALUES (%s, %s, %s, %s, %s, %s)"
        arguments = [None, own_id, summary, version, created_at, updated_at]
        self.data_source.execute_and_commit(query, arguments)

    def __insert_issue_tracker(self, repo_id, type, url):
        query = "INSERT IGNORE INTO issue_tracker " \
                "VALUES (%s, %s, %s, %s)"
        arguments = [None, repo_id, url, type]
        self.data_source.execute_and_commit(query, arguments)

    def __select_issue_tracker_id(self, repo_id, url):
        query = "SELECT id " \
                "FROM issue_tracker " \
                "WHERE url = %s"
        arguments = [url]
        row = self.data_source.get_row(query, arguments)
        if row:
            repo_id = row[0]
        else:
            self.logger("no issue tracker linked to " + str(url))
        return repo_id
