#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'agustin ventura'

import logging
import sys

sys.path.insert(0, "..\\..")
from querier_github import GithubQuerier


class IssuePageReader:
    def __init__(self, access_token, github_repo_name, issues_pages):
        self.access_token = access_token
        self.issues_pages = issues_pages
        self.github_repo_name = github_repo_name
        self.github_querier = GithubQuerier(github_repo_name, access_token)

    def __call__(self):
        issues = []
        for issue_page in self.issues_pages:
            logging.info("Reading page " + str(issue_page))
            issues.append(self.github_querier.read_page(issue_page))
        return issues
