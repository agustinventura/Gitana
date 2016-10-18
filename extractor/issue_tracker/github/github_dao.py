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
        issue_tracker_id = self.__select_issue_tracker_id(url)
        if issue_tracker_id is None:
            self.__insert_issue_tracker(repo_id, type, url)
            issue_tracker_id = self.__select_issue_tracker_id(url)
        return issue_tracker_id

    def get_user_id(self, user_name, user_email):
        user_id = self.db_util.select_user_id_by_name(self.data_source.get_connection(), user_name, self.logger)
        if user_id is None:
            self.db_util.insert_user(self.data_source.get_connection(), user_name, user_email, self.logger)
            user_id = self.db_util.select_user_id_by_name(self.data_source.get_connection(), user_name, self.logger)
        return user_id

    def get_user_name(self, user_id):
        query = "SELECT name FROM user WHERE id = %s"
        arguments = [user_id]
        row = self.data_source.get_row(query, arguments)
        user_name = None
        if row:
            user_name = row[0]
        else:
            self.logger.warning("No user with id " + user_id)
        return user_name

    def get_event_type_id(self, event_type):
        event_type_id = self.__select_event_type(event_type)
        if event_type_id is None:
            self.__insert_event_type(event_type)
            event_type_id = self.__select_event_type(event_type)
        return event_type_id

    def get_commit_id(self, sha):
        query = "SELECT id FROM commit WHERE sha = %s"
        arguments = [sha]
        row = self.data_source.get_row(query, arguments)
        commit_id = None
        if row:
            commit_id = row[0]
        else:
            self.logger.warning("No commit with sha " + str(sha))
        return commit_id

    def get_comment_user_id(self, comment_created_at):
        query = "SELECT author_id FROM message WHERE created_at = %s"
        arguments = [comment_created_at]
        row = self.data_source.get_row(query, arguments)
        author_id = None
        if row:
            author_id = row[0]
        else:
            self.logger.warning("No comment with created_at " + str(comment_created_at))
        return author_id

    def insert_issue(self, own_id, summary, version, user_id, created_at, updated_at):
        query = "INSERT IGNORE INTO issue(id, own_id, summary, version, reporter_id, created_at, last_change_at) " \
                "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        arguments = [None, own_id, summary, version, user_id, created_at, updated_at]
        self.data_source.execute_and_commit(query, arguments)
        query = "SELECT id FROM issue WHERE own_id like %s AND created_at like %s"
        arguments = [own_id, created_at]
        row = self.data_source.get_row(query, arguments)
        if row:
            issue_id = row[0]
        else:
            self.logger.warning("No issue with own_id " + str(own_id) + " and created at " + str(created_at))
        return issue_id

    def insert_issue_label(self, issue_id, label_name):
        label_id = self.__insert_label(label_name)
        self.__insert_issue_label(issue_id, label_id)

    def insert_issue_comment(self, issue_id, user_id, comment_id, comment_body, comment_created_at):
        query = "INSERT IGNORE INTO message(id, own_id, body, created_at, author_id, issue_id) " \
                "VALUES (%s, %s, %s, %s, %s, %s)"
        arguments = [None, comment_id, comment_body, comment_created_at, user_id, issue_id]
        self.data_source.execute_and_commit(query, arguments)

    def insert_issue_event(self, issue_id, event_type_id, detail, creator_id, created_at, target_user_id):
        query = "INSERT IGNORE INTO issue_event(id, issue_id, event_type_id, detail, creator_id, created_at, target_user_id) " \
                "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        arguments = [None, issue_id, event_type_id, detail, creator_id, created_at, target_user_id]
        self.data_source.execute_and_commit(query, arguments)

    def insert_issue_commit(self, issue_id, commit_id):
        query = "INSERT IGNORE INTO issue_commit_dependency(issue_id, commit_id) values (%s, %s)"
        arguments = [issue_id, commit_id]
        self.data_source.execute_and_commit(query, arguments)

    def insert_issue_subscriber(self, issue_id, user_id):
        query = "INSERT IGNORE INTO issue_subscriber(issue_id, subscriber_id) values (%s, %s)"
        arguments = [issue_id, user_id]
        self.data_source.execute_and_commit(query, arguments)

    def insert_issue_assignee(self, issue_id, user_id):
        query = "INSERT IGNORE INTO issue_assignee(issue_id, assignee_id) values (%s, %s)"
        arguments = [issue_id, user_id]
        self.data_source.execute_and_commit(query, arguments)

    def __insert_label(self, label_name):
        query = "INSERT IGNORE INTO label(id, name) " \
                "VALUES (%s, %s)"
        arguments = [None, label_name.lower()]
        self.data_source.execute_and_commit(query, arguments)
        query = "SELECT id FROM label WHERE name like %s"
        arguments = [label_name.lower()]
        row = self.data_source.get_row(query, arguments)
        if row:
            label_id = row[0]
        else:
            self.logger.warning("No label with name " + str(label_name))
        return label_id

    def __insert_issue_tracker(self, repo_id, type, url):
        query = "INSERT IGNORE INTO issue_tracker " \
                "VALUES (%s, %s, %s, %s)"
        arguments = [None, repo_id, url, type]
        self.data_source.execute_and_commit(query, arguments)

    def __select_issue_tracker_id(self, url):
        issue_tracker_id = None
        query = "SELECT id " \
                "FROM issue_tracker " \
                "WHERE url = %s"
        arguments = [url]
        row = self.data_source.get_row(query, arguments)
        if row:
            issue_tracker_id = row[0]
        else:
            self.logger.warning("No issue tracker with url " + str(url))
        return issue_tracker_id

    def __insert_issue_label(self, issue_id, label_id):
        query = "INSERT IGNORE INTO issue_labelled(issue_id, label_id) " \
                "VALUES (%s, %s)"
        arguments = [issue_id, label_id]
        self.data_source.execute_and_commit(query, arguments)

    def __insert_event_type(self, event_type):
        query = "INSERT IGNORE INTO issue_event_type " \
                "VALUES (%s, %s)"
        arguments = [None, event_type]
        self.data_source.execute_and_commit(query, arguments)

    def __select_event_type(self, event_type):
        event_type_id = None
        query = "SELECT id " \
                "FROM issue_event_type " \
                "WHERE name = %s"
        arguments = [event_type]
        row = self.data_source.get_row(query, arguments)
        if row:
            event_type_id = row[0]
        else:
            self.logger.warning("No event type with name " + str(event_type))
        return event_type_id
