#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'agustin ventura'

import sys
sys.path.insert(0, "..\\..")

from github import Github


# I'm mixing here two concepts: Data access object and iterator, should divide them
class GithubQuerier:
    def __init__(self, repo_name, access_token, logger):
        self.logger = logger
        self.repo_name = repo_name
        self.github = Github(access_token)
        self.repository = self.__load_repository()
        self.issues = None
        self.last_page = -1
        self.current_page = 0
        self.issues_initialized = False

    def __load_repository(self):
        repository = None
        try:
            repository = self.github.get_repo(self.repo_name)
        except Exception, e:
            self.logger.error("Error loading repository " + self.repo_name + ": " + e.message)
        return repository

    def load_issues_page(self):
        if not self.issues_initialized:
            self.__load_issues()

        if self.current_page <= self.last_page:
            issues_page = self.issues.get_page(self.current_page)
            self.current_page += 1
        else:
            issues_page = None

        return issues_page

    def __load_issues(self):
        if self.repository is not None:
            self.issues = self.__get_all_issues_ascending()
            self.last_page = self.__get_last_page()
            self.current_page = 0
            self.issues_initialized = True
        else:
            self.logger("Error loading issues, repository " + self.repo_name + "not initialized")

    def __get_last_page(self):
        last_page_url = self.issues._getLastPageUrl()
        if last_page_url is not None:
            last_page = int(last_page_url.split("page=")[-1])
        else:
            last_page = 0
        return last_page

    def __get_all_issues_ascending(self):
        return self.repository.get_issues(state="all", direction="asc")

