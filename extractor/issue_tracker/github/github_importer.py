#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'agustin ventura'

import sys

sys.path.insert(0, "..\\..")
from github_querier import GithubQuerier
from github_dao import GithubDAO
from issue_writer import IssueWriter


class GithubImporter:
    def __init__(self, db_name, project_name, repo_name, url, github_repo_name, access_token, config, logger):
        self.logger = logger
        self.log_path = self.logger.name.rsplit('.', 1)[0] + "-" + project_name
        self.type = "github"
        self.url = url
        self.project_name = project_name
        self.db_name = db_name
        self.repo_name = repo_name
        self.repo_id = None
        self.github_repo_name = github_repo_name
        config.update({'database': db_name})
        self.github_reader = GithubQuerier(self.github_repo_name, access_token, self.logger)
        self.github_dao = GithubDAO(config, self.logger)

    def import_issues(self):
        # 1) Init database
        self.repo_id = self.github_dao.get_repo_id(self.project_name, self.repo_name)
        issue_tracker_id = self.github_dao.get_issue_tracker_id(self.repo_id, self.url, self.type)
        # 2) Read all the issues
        issue_writer = IssueWriter(self.github_reader, self.github_dao, issue_tracker_id, self.logger)
        issues = self.github_reader.load_issues_page()
        while issues is not None:
            for issue in issues:
                # 3) Write the issue
                issue_writer.write(issue)
            issues = self.github_reader.load_issues_page()

