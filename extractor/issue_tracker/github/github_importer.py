#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'agustin ventura'

import sys

sys.path.insert(0, "..\\..")
from github_reader import GithubReader
from github_dao import GithubDAO


class GithubImporter:
    def __init__(self, db_name, project_name, repo_name, url, github_repo_name, config, logger):
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
        self.github_reader = GithubReader(self.github_repo_name, self.logger)
        self.github_dao = GithubDAO(config, self.logger)

    def import_issues(self):
        # 1) Init database
        self.repo_id = self.github_dao.get_repo_id(self.project_name, self.repo_name)
        issue_tracker_id = self.github_dao.get_issue_tracker_id(self.repo_id, self.url, self.type)
        # 2) Read all the issues
        issues = self.github_reader.load_issues_page()
        while issues is not None:
            for issue in issues:
                print issue
            issues = self.github_reader.load_issues_page()

            # 3) Read every data issue
