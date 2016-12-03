#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'agustin ventura'

import logging
import sys

sys.path.insert(0, "..\\..")
from github_dao import GithubDAO


class GithubIssueDependency2Db:
    def __init__(self, issue_tracker_id, comments, config, github_querier):
        self.issue_tracker_id = issue_tracker_id
        self.comments = comments
        self.config = config
        self.github_querier = github_querier
        self.github_dao = None

    def __call__(self):
        self.github_dao = GithubDAO(self.config)
        for comment in self.comments:
            logging.info("Analyzing comment " + str(comment["id"]))
            self.write_issue_reference(comment["body"], comment["issue_id"])

    def write_issue_reference(self, body, issue_id):
        referenced_issues_own_id = self.github_querier.read_issue_references(body)
        for referenced_issue_own_id in referenced_issues_own_id:
            referenced_issue_id = self.github_dao.get_issue_id_by_own_id(referenced_issue_own_id[1:],
                                                                         self.issue_tracker_id)
            if referenced_issue_id is not None:
                self.github_dao.insert_issue_reference(issue_id, referenced_issue_id)
            else:
                logging.warning(
                    "Issue " + str(issue_id) + " references issue with own_id " + str(referenced_issue_own_id)
                    + " but no one exists")
