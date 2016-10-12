#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'agustin ventura'

import sys

sys.path.insert(0, "..\\..")
from extractor.util.date_util import DateUtil


class IssueWriter:
    def __init__(self, github_reader, github_dao, issue_tracker_id, logger):
        self.github_reader = github_reader
        self.github_dao = github_dao
        self.issue_tracker_id = issue_tracker_id
        self.logger = logger
        self.date_util = DateUtil()

    def write(self, issue):
        user_id = self.__write_user(issue)
        if user_id is not None:
            issue_id = self.__write_issue(issue, user_id)
            self.__write_labels(issue, issue_id)
            # 2) Comments
            # 3) History/Events
            # 4) Subscribers
            # 5) Asignee
            # 6) Issue-commit-dependency
        else:
            self.logger.info("Skipped issue " + str(issue.number) + " user has no name or email")

    def __write_user(self, issue):
        user_id = None
        if issue.user.name is not None and issue.user.email is not None:
            user_id = self.github_dao.get_user_id(issue.user)
        return user_id

    def __write_issue(self, issue, user_id):
        own_id = issue.number
        summary = issue.title
        version = self.read_version(issue)
        created_at = self.date_util.get_timestamp(issue.created_at, "%Y-%m-%d %H:%M:%S")
        updated_at = self.date_util.get_timestamp(issue.updated_at, "%Y-%m-%d %H:%M:%S")
        issue_id = self.github_dao.insert_issue(own_id, summary, version, user_id, created_at, updated_at)
        return issue_id

    def read_version(self, issue):
        version = None
        if issue.milestone is not None:
            version = issue.milestone.number
        return version

    def __write_labels(self, issue, issue_id):
        for label in issue.get_labels():
            if label.name is not None:
                self.github_dao.insert_issue_label(issue_id, label)
            else:
                self.logger("Skipping label " + str(label) + ", label has no name")
