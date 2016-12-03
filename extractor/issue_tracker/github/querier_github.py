#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time

from github import GithubException

from util.date_util import DateUtil

__author__ = 'agustin ventura'

import logging
import re
import sys

sys.path.insert(0, "..\\..")

from github import Github


class GithubQuerier:
    def __init__(self, repo_name, access_token):
        self.repo_name = repo_name
        self.github = Github(access_token)
        self.repository = self.__load_repository()
        self.date_util = DateUtil()
        self.issues = None
        self.last_page = -1
        self.current_page = 0
        self.issues_initialized = False
        self.issue_reference_pattern = '\s#\d+'
        self.issue_attachment_pattern = '(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](?:com|net|org|edu|gov' \
                                        '|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|' \
                                        'travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|' \
                                        'be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|' \
                                        'cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|' \
                                        'fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|' \
                                        'hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|' \
                                        'kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|' \
                                        'mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|' \
                                        'nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|' \
                                        'sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|' \
                                        'tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|' \
                                        'wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)' \
                                        '[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|' \
                                        '[^\s`!()\[\]{};:\'".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.]' \
                                        '(?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|' \
                                        'name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|' \
                                        'aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|' \
                                        'cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|' \
                                        'eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|' \
                                        'gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|' \
                                        'jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|' \
                                        'md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|' \
                                        'ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|' \
                                        'rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|' \
                                        'sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|' \
                                        'vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))'

    def __load_repository(self):
        repository = None
        try:
            repository = self.github.get_repo(self.repo_name)
        except Exception, e:
            logging.error("Error loading repository " + self.repo_name + ": " + e.message)
        return repository

    def load_issues_page(self, last_date):
        if not self.issues_initialized:
            self.__load_issues(last_date)

        if self.current_page <= self.last_page:
            issues_page = self.issues.get_page(self.current_page)
            self.current_page += 1
        else:
            issues_page = None

        return issues_page

    def load_issue(self, issue_id):
        return self.repository.get_issue(issue_id)

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

    def read_issue_references(self, body):
        if body is not None:
            return re.findall(self.issue_reference_pattern, body)
        else:
            return []

    def read_issue_attachments(self, body):
        if body is not None:
            return re.findall(self.issue_attachment_pattern, body)
        else:
            return []

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

    def __load_issues(self, last_date):
        if self.repository is not None:
            if last_date is None:
                self.issues = self.__get_all_issues_ascending()
            else:
                self.issues = self.__get_new_issues_ascending(last_date)
            self.issues_initialized = True
            self.__read_last_page()
            self.current_page = 0
        else:
            logging.error("Error loading issues, repository " + self.repo_name + "not initialized")

    def get_last_page(self):
        if not self.issues_initialized:
            self.__load_issues(None)

        if self.last_page is None:
            self.__read_last_page()
        return self.last_page

    def __read_last_page(self):
        last_page_url = self.issues._getLastPageUrl()
        if last_page_url is not None:
            self.last_page = int(last_page_url.split("page=")[-1])
        else:
            self.last_page = 0

    def __get_all_issues_ascending(self):
        return self.repository.get_issues(state="all", direction="asc")

    def __get_new_issues_ascending(self, last_date):
        return self.repository.get_issues(state="all", direction="asc",
                                          since=self.date_util.get_timestamp(last_date, "%Y-%m-%d %H:%M:%S"))

    def read_page(self, page):
        if not self.issues_initialized:
            self.__load_issues(None)

        if page <= self.last_page:
            issues_page = self.issues.get_page(page)

        return issues_page


class IssuePageReader:
    def __init__(self, access_token, github_repo_name, issues_pages):
        self.access_token = access_token
        self.issues_pages = issues_pages
        self.github_repo_name = github_repo_name
        self.github_querier = GithubQuerier(github_repo_name, access_token)

    def __call__(self):
        issues = []
        i = 0
        while i < len(self.issues_pages):
            issue_page = self.issues_pages[i]
            logging.info("Reading page " + str(issue_page))
            try:
                issues.append(self.github_querier.read_page(issue_page))
                i += 1
            except GithubException as e:
                if e.status == 403:
                    logging.info(
                        "Exceded token capacity while reading issue page " + str(issue_page) + ". Awaiting one hour")
                    time.sleep(3600)
                    logging.info("Restarting issue page reading")
                else:
                    logging.error("Caught unknown exception while reading pages: " + str(e))
            except:
                e = sys.exc_info()[0]
                logging.error("Caught unknown exception while reading issue page " + str(issue_page) + ": " + e.message)
                i += 1
        return issues


class IssueReader:
    def __init__(self, access_token, github_repo_name, issues):
        self.access_token = access_token
        self.issues = issues
        self.github_repo_name = github_repo_name
        self.github_querier = GithubQuerier(github_repo_name, access_token)

    def __call__(self):
        issues = []
        i = 0
        while i < len(self.issues):
            issue = self.issues[i]
            logging.info("Reading issue " + str(issue))
            try:
                issues.append(self.github_querier.load_issue(int(issue)))
                i += 1
            except GithubException as e:
                if e.status == 403:
                    logging.info(
                        "Exceded token capacity while reading issue " + str(issue) + ". Awaiting one hour")
                    time.sleep(3600)
                    logging.info("Restarting issue reading")
                else:
                    logging.error("Caught unknown exception while reading pages: " + str(e))
            except:
                e = sys.exc_info()[0]
                logging.error("Caught unknown exception while reading issue " + str(issue) + ": " + e.message)
                i += 1
        return issues
