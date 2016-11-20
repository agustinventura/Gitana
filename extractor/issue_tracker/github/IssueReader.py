#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'agustin ventura'

import logging
import sys

sys.path.insert(0, "..\\..")
from querier_github import GithubQuerier


class IssueReader:
    def __init__(self, access_token, github_repo_name, issues):
        self.access_token = access_token
        self.issues = issues
        self.github_repo_name = github_repo_name
        self.github_querier = GithubQuerier(github_repo_name, access_token)

    def __call__(self):
        issues = []
        for issue in self.issues:
            logging.info("Reading issue " + str(issue))
            issues.append(self.github_querier.load_issue(int(issue)))
        return issues
