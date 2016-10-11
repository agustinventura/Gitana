#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'agustin ventura'

import sys
sys.path.insert(0, "..\\..")

from github import Github
import logging
import logging.handlers

USERNAME = "gabrielecirulli"
REPO_NAME = "2048"


class GitHubQuerier:

    def __init__(self, username, repo_name, logger):
        self.logger = logger
        self.username = username
        self.repo_name = repo_name
        self.github = None
        self.repository = None

    def load_repository(self):
            try:
                self.repository = self.github.get_repo(USERNAME+"/"+REPO_NAME)
            except Exception, e:
                self.logger.error("Error loading user " + USERNAME + " repository " + REPO_NAME + ": " + e.message)

    def extract(self):
        self.github = Github()
        self.load_repository()
        self.load_issues()

    def load_issues(self):
        if self.repository is not None:
            last_page = self.get_last_page()
            page_count = 0
            while page_count <= last_page:
                issues = self.get_all_issues_ascending().get_page(page_count)
                for issue in issues:
                    print issue
                page_count += 1
        else:
            self.logger("Error loading issues, repository " + REPO_NAME + "does not exist")

    def get_last_page(self):
        last_page_url = self.get_all_issues_ascending()._getLastPageUrl()
        last_page = int(last_page_url.split("page=")[-1])
        return last_page

    def get_all_issues_ascending(self):
        return self.repository.get_issues(state="all", direction="asc")


def main():
    logger = get_stdout_logger()
    github_querier = GitHubQuerier(USERNAME, REPO_NAME, logger)
    github_querier.extract()


def get_stdout_logger():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)
    return root


if __name__ == "__main__":
    main()