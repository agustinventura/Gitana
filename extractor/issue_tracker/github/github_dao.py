#!/usr/bin/env python
# -*- coding: utf-8 -*-
from extractor.util.data_source import DataSource

from util.db_util import DbUtil

__author__ = 'agustin ventura'

import logging


class GithubDAO:
    def __init__(self, config):
        self.config = config
        self.data_source = DataSource(config)
        self.db_util = DbUtil()

    def __del__(self):
        logging.debug("GithubDAO destroyed")
        self.close()

    def close(self):
        if self.data_source is not None:
            self.data_source.close_connection()

    def get_repo_id(self, project_name, repo_name):
        project_id = self.db_util.select_project_id(self.data_source.open_connection(), project_name,
                                                    logging.getLogger())
        repo_id = self.db_util.select_repo_id(self.data_source.open_connection(), repo_name,
                                              logging.getLogger())
        return repo_id

    def get_issue_tracker_id(self, repo_id, url, type):
        issue_tracker_id = self.__select_issue_tracker_id(url)
        if issue_tracker_id is None:
            self.__insert_issue_tracker(repo_id, type, url)
            issue_tracker_id = self.__select_issue_tracker_id(url)
        return issue_tracker_id

    def get_user_id_by_email(self, email):
        query = "SELECT id FROM user WHERE email = %s"
        arguments = [email]
        row = self.data_source.get_row(query, arguments)
        user_id = None
        if row:
            user_id = row[0]
        else:
            logging.warning("No user with email " + email)
        return user_id

    def get_user_id_by_login(self, login):
        query = "SELECT id FROM user WHERE name = %s"
        arguments = [login]
        row = self.data_source.get_row(query, arguments)
        user_id = None
        if row:
            user_id = row[0]
        else:
            logging.warning("No user with login " + login)
        return user_id

    def get_user_name(self, user_id):
        query = "SELECT name FROM user WHERE id = %s"
        arguments = [user_id]
        row = self.data_source.get_row(query, arguments)
        user_name = None
        if row:
            user_name = row[0]
        else:
            logging.warning("No user with id " + user_id)
        return user_name

    def insert_user(self, login, email):
        self.db_util.insert_user(self.data_source.open_connection(), login, email, logging.getLogger())
        user_id = self.db_util.select_user_id_by_name(self.data_source.open_connection(), login,
                                                      logging.getLogger())
        return user_id

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
            logging.warning("No commit with sha " + str(sha))
        return commit_id

    def get_commit_user_id(self, sha):
        query = "SELECT author_id FROM commit WHERE sha = %s"
        arguments = [sha]
        row = self.data_source.get_row(query, arguments)
        author_id = None
        if row:
            author_id = row[0]
        else:
            logging.warning("No commit with sha " + str(sha))
        return author_id

    def get_comment_user_id(self, comment_created_at):
        query = "SELECT author_id FROM message WHERE created_at = %s"
        arguments = [comment_created_at]
        row = self.data_source.get_row(query, arguments)
        author_id = None
        if row:
            author_id = row[0]
        else:
            logging.warning("No comment with created_at " + str(comment_created_at))
        return author_id

    def get_issue_id_by_own_id(self, own_id, issue_tracker_id):
        query = "SELECT id FROM issue WHERE own_id = %s and issue_tracker_id = %s"
        arguments = [own_id, issue_tracker_id]
        row = self.data_source.get_row(query, arguments)
        issue_id = None
        if row:
            issue_id = row[0]
        else:
            logging.warning("No issue with own_id " + str(own_id))
        return issue_id

    def get_own_ids(self, issue_tracker_id):
        query = "SELECT own_id FROM issue WHERE issue_tracker_id = %s"
        arguments = [issue_tracker_id]
        rows = self.data_source.get_rows(query, arguments)
        issue_ids = []
        if rows:
            for row in rows:
                issue_ids.append(row[0])
        else:
            logging.warning("No issues for issue tracker " + str(issue_tracker_id))
        return issue_ids

    def insert_issue(self, user_id, own_id, summary, version, created_at, updated_at, issue_tracker_id):
        query = "INSERT IGNORE INTO issue(id, own_id, summary, version, reporter_id, issue_tracker_id, created_at, " \
                "last_change_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        arguments = [None, own_id, summary, version, user_id, issue_tracker_id, created_at, updated_at]
        self.data_source.execute_and_commit(query, arguments)
        query = "SELECT id FROM issue WHERE own_id like %s AND created_at like %s"
        arguments = [own_id, created_at]
        row = self.data_source.get_row(query, arguments)
        if row:
            issue_id = row[0]
        else:
            logging.warning("No issue with own_id " + str(own_id) + " and created at " + str(created_at))
        return issue_id

    def update_issue(self, issue_id, user_id, summary, version, created_at, updated_at):
        query = "UPDATE issue SET summary = %s, version = %s, reporter_id = %s, created_at = %s, last_change_at = %s" \
                "WHERE id = %s"
        arguments = [summary, version, user_id, created_at, updated_at, issue_id]
        self.data_source.execute_and_commit(query, arguments)

    def insert_issue_label(self, issue_id, label_name):
        label_id = self.__insert_label(label_name)
        self.__insert_issue_label(issue_id, label_id)

    def delete_issue_labels(self, issue_id):
        query = "DELETE FROM issue_labelled WHERE issue_id = %s"
        arguments = [issue_id]
        self.data_source.execute_and_commit(query, arguments)

    def insert_issue_comment(self, issue_id, user_id, comment_id, comment_pos, comment_body, comment_created_at):
        query = "INSERT IGNORE INTO message(id, own_id, pos, body, created_at, author_id, issue_id) " \
                "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        arguments = [None, comment_id, comment_pos, comment_body, comment_created_at, user_id, issue_id]
        self.data_source.execute_and_commit(query, arguments)
        query = "SELECT id FROM message WHERE own_id = %s AND issue_id = %s"
        arguments = [comment_id, issue_id]
        row = self.data_source.get_row(query, arguments)
        if row:
            comment_id = row[0]
        else:
            logging.warning("No comment with own_id " + str(comment_id) + " and issue_id " + str(issue_id))
        return comment_id

    def delete_issue_comments(self, issue_id):
        query = "DELETE FROM message WHERE issue_id = %s"
        arguments = [issue_id]
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

    def delete_issue_assignee(self, issue_id):
        query = "DELETE FROM issue_assignee WHERE issue_id = %s"
        arguments = [issue_id]
        self.data_source.execute_and_commit(query, arguments)

    def insert_issue_reference(self, issue_id, referenced_issue_id):
        query = "INSERT IGNORE INTO issue_dependency(issue_source_id, issue_target_id, type_id) values (%s, %s, %s)"
        arguments = [issue_id, referenced_issue_id, 3]
        self.data_source.execute_and_commit(query, arguments)

    def delete_issue_reference(self, issue_id):
        query = "DELETE FROM issue_dependency WHERE issue_source_id = %s"
        arguments = [issue_id]
        self.data_source.execute_and_commit(query, arguments)

    def insert_issue_attachment(self, comment_id, attachment_url):
        query = "INSERT IGNORE INTO attachment(id, message_id, url) values (%s, %s, %s)"
        arguments = [None, comment_id, attachment_url]
        self.data_source.execute_and_commit(query, arguments)

    def delete_issue_attachment(self, issue_id):
        query = "DELETE FROM attachment WHERE message_id = %s"
        arguments = [issue_id]
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
            logging.warning("No label with name " + str(label_name))
        return label_id

    def __insert_issue_tracker(self, repo_id, type, url):
        query = "INSERT IGNORE INTO issue_tracker(id, repo_id, name, type) " \
                "VALUES (%s, %s, %s, %s)"
        arguments = [None, repo_id, url, type]
        self.data_source.execute_and_commit(query, arguments)

    def __select_issue_tracker_id(self, url):
        issue_tracker_id = None
        query = "SELECT id " \
                "FROM issue_tracker " \
                "WHERE name = %s"
        arguments = [url]
        row = self.data_source.get_row(query, arguments)
        if row:
            issue_tracker_id = row[0]
        else:
            logging.warning("No issue tracker with url " + str(url))
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
            logging.warning("No event type with name " + str(event_type))
        return event_type_id

    def get_issue_comments(self, issue_tracker_id):
        query = "SELECT m.id, m.issue_id, m.body FROM message m JOIN issue i ON m.issue_id = i.id and i.issue_tracker_id = %s"
        arguments = [issue_tracker_id]
        rows = self.data_source.get_rows(query, arguments)
        messages = []
        for row in rows:
            message = {
                "id": int(row[0]),
                "issue_id": int(row[1]),
                "body": str(row[2])
            }
            messages.append(message)
        return messages
