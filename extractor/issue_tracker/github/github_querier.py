#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'agustin ventura'

import re
import sys

sys.path.insert(0, "..\\..")

from github import Github
from extractor.util.date_util import DateUtil


# I'm mixing here two concepts: Data access object and iterator, should divide them
class GithubQuerier:
    def __init__(self, repo_name, access_token, logger):
        self.logger = logger
        self.repo_name = repo_name
        self.github = Github(access_token)
        self.repository = self.__load_repository()
        self.date_util = DateUtil()
        self.issues = None
        self.last_page = -1
        self.current_page = 0
        self.issues_initialized = False
        self.issue_reference_pattern = re.compile('\s#\d+\s')

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

    def read_user(self, issue):
        user_data = {"login": issue.user.login,
                     "email": issue.user.email}
        return user_data

    def read_issue(self, issue):
        issue_data = {"own_id": issue.number,
                      "summary": issue.title,
                      "body": issue.body}
        issue_data["version"] = self.__read_version(issue)
        issue_data["created_at"] = self.date_util.get_timestamp(issue.created_at, "%Y-%m-%d %H:%M:%S")
        issue_data["updated_at"] = self.date_util.get_timestamp(issue.updated_at, "%Y-%m-%d %H:%M:%S")
        return issue_data

    def read_comments(self, issue):
        return issue.get_comments()

    def read_comment(self, comment):
        comment_data = {"user": comment.user,
                        "id": comment.id,
                        "body": comment.body}
        comment_data["created_at"] = self.date_util.get_timestamp(comment.created_at, "%Y-%m-%d %H:%M:%S")
        return comment_data

    def read_issue_reference(self, body):
        referenced_issue_own_id = None
        issue_reference = self.issue_reference_pattern.search(body)
        if issue_reference is not None:
            referenced_issue_own_id = issue_reference.group()[2:]
        return referenced_issue_own_id

    def read_labels(self, issue):
        return [label.name for label in issue.get_labels() if label.name is not None]

    def read_events(self, issue):
        return issue.get_events()

    def read_event(self, event):
        event_data = {"actor": event.actor,
                      "event": event.event,
                      "created_at": self.date_util.get_timestamp(event.created_at, "%Y-%m-%d %H:%M:%S"),
                      "commit_id": event.commit_id}
        if event._rawData.get('assignee') is not None:
            event_data["assignee"] = event._rawData.get('assignee').get('login')
        if event._rawData.get('milestone') is not None:
            event_data["milestone"] = event._rawData.get('milestone').get('title')
        if event._rawData.get('label') is not None:
            event_data["label"] = event._rawData.get('label').get('name')
        return event_data

    def read_actor(self, event):
        actor_data = {"login": event.actor.login,
                      "email": event.actor.email}
        return actor_data

    def read_user_data(self, user):
        actor_data = {"login": user.login,
                      "email": user.email}
        return actor_data

    def __read_version(self, issue):
        version = None
        if issue.milestone is not None:
            version = issue.milestone.number
        return version

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

