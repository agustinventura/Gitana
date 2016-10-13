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
        user_id = self.__write_user(issue.user)
        issue_id = self.__write_issue(issue, user_id)
        self.__write_labels(issue, issue_id)
        self.__write_comments(issue, issue_id)
        self.__write_events(issue, issue_id)
        # 4) Subscribers (we can get it as the actors in subscribed events in issues)
        # 5) Asignee
        # 6) Issue-commit-dependency (we can get it from referenced/merged events in issues)

    def __write_user(self, user):
        user_id = self.github_dao.get_user_id(user.login, user.email)
        return user_id

    def __write_issue(self, issue, user_id):
        own_id = issue.number
        summary = issue.title
        version = self.read_version(issue)
        created_at = self.date_util.get_timestamp(issue.created_at, "%Y-%m-%d %H:%M:%S")
        updated_at = self.date_util.get_timestamp(issue.updated_at, "%Y-%m-%d %H:%M:%S")
        issue_id = self.github_dao.insert_issue(own_id, summary, version, user_id, created_at, updated_at)
        return issue_id

    def read_version(self, issue):
        version = None
        if issue.milestone is not None:
            version = issue.milestone.number
        return version

    def __write_labels(self, issue, issue_id):
        for label in issue.get_labels():
            if label.name is not None:
                self.github_dao.insert_issue_label(issue_id, label.name)
            else:
                self.logger.warning("Skipping label " + str(label) + ", label has no name")

    def __write_comments(self, issue, issue_id):
        for comment in issue.get_comments():
            user_id = self.__write_user(comment.user)
            comment_id = comment.id
            comment_body = comment.body
            comment_created_at = self.date_util.get_timestamp(comment.created_at, "%Y-%m-%d %H:%M:%S")
            self.github_dao.insert_issue_comment(issue_id, user_id, comment_id, comment_body, comment_created_at)

    def __write_events(self, issue, issue_id):
        for event in issue.get_events():
            if event.actor is not None:
                creator_id = self.__write_user(event.actor)
                event_type_id = self.github_dao.get_event_type_id(event.event)
                created_at = self.date_util.get_timestamp(event.created_at, "%Y-%m-%d %H:%M:%S")
                detail = self.__get_issue_event_detail(issue_id, event)
                target_user_id = self.__get_target_user_id(event)
                self.github_dao.insert_issue_event(issue_id, event_type_id, detail, creator_id, created_at,
                                                   target_user_id)
            else:
                self.logger.warning("Skipped event " + str(event.id) + " because it has no actor")

    def __get_issue_event_detail(self, issue_id, event):
        detail = None
        event_type = event.event
        if event_type == "assigned" or event_type == "unassigned":
            detail = event.actor.login + " " + event_type + " issue " + str(issue_id) + " to " + event._rawData.get(
                'assignee').get('login')
        elif event_type == "labeled" or event_type == "unlabeled":
            detail = event.actor.login + " " + event_type + " issue " + str(issue_id) + " with " + event._rawData.get(
                'label').get('name')
        elif event_type == "closed" or event_type == "merged" or event_type == "referenced":
            detail = event.actor.login + " " + event_type + " issue " + str(issue_id)
            if event.commit_id is not None:
                detail += " with commit " + event.commit_id
            else:
                detail += " without commit"
        elif event_type == "milestoned" or event_type == "demilestoned":
            detail = event.actor.login + " " + event_type + " issue " + str(issue_id) + " with " + event._rawData.get(
                'milestone').get('title')
        elif event_type == "mentioned":
            detail = event.actor.login + " " + event_type + " in issue " + str(issue_id)
        else:
            detail = event.actor.login + " " + event_type + " issue " + str(issue_id)
        return detail

    def __get_target_user_id(self, event):
        target_user_id = None
        if event.event == "assigned" or event.event == "unassigned":
            assignee = event._rawData.get('assignee').get('login')
            target_user_id = self.github_dao.get_user_id(assignee, None)
        return target_user_id
