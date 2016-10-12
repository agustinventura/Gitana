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
        self.logger.info("Writing issue " + str(issue.number))
        own_id = issue.id
        summary = issue.title
        version = None
        version = self.read_version(issue, version)
        if issue.user.name is not None and issue.user.email is not None:
            user_id = self.github_dao.get_user_id(issue.user)
            created_at = self.date_util.get_timestamp(issue.created_at, "%Y-%m-%d %H:%M:%S")
            updated_at = self.date_util.get_timestamp(issue.updated_at, "%Y-%m-%d %H:%M:%S")
            self.github_dao.insert_issue(own_id, summary, version, user_id, created_at, updated_at)
        else:
            self.logger.info("Skipped issue " + str(issue.number) + " user has no name or email")

    def read_version(self, issue, version):
        if issue.milestone is not None:
            version = issue.milestone.number
        return version
