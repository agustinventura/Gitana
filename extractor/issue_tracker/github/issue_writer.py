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
        self.__write_assignees(issue, issue_id)

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
                issue_event = {"issue_id": issue_id}
                issue_event["creator_id"] = self.__write_user(event.actor)
                issue_event["event_type_id"] = self.github_dao.get_event_type_id(event.event)
                issue_event["created_at"] = self.date_util.get_timestamp(event.created_at, "%Y-%m-%d %H:%M:%S")
                issue_event["target_user_id"] = None
                self.__process_event(event, issue_event)
                self.github_dao.insert_issue_event(issue_event["issue_id"], issue_event["event_type_id"],
                                                   issue_event["detail"], issue_event["creator_id"],
                                                   issue_event["created_at"], issue_event["target_user_id"])
            else:
                self.logger.warning("Skipped event " + str(event.id) + " because it has no actor")

    def __process_event(self, event, issue_event):
        event_type = event.event
        if event_type == "assigned" or event_type == "unassigned":
            self.__process_assigned_event(event, event_type, issue_event)
        elif event_type == "labeled" or event_type == "unlabeled":
            self.__process_labeled_event(event, event_type, issue_event)
        elif event_type == "closed" or event_type == "merged" or event_type == "referenced":
            self.__process_commit_related_event(event, event_type, issue_event)
        elif event_type == "milestoned" or event_type == "demilestoned":
            self.__process_milestone_event(event, event_type, issue_event)
        elif event_type == "mentioned":
            self.__process_mentioned_event(event, event_type, issue_event)
        elif event_type == "subscribed":
            self.__process_subscription_event(event, event_type, issue_event)
        else:
            self.__process_generic_event(event, event_type, issue_event)

    def __process_generic_event(self, event, event_type, issue_event):
        issue_event["detail"] = event.actor.login + " " + event_type + " issue " + str(issue_event["issue_id"])

    def __process_subscription_event(self, event, event_type, issue_event):
        self.__process_generic_event(event, event_type, issue_event)
        self.github_dao.insert_issue_subscriber(issue_event["issue_id"], issue_event["creator_id"])

    def __process_mentioned_event(self, event, event_type, issue_event):
        event_created_at = issue_event["created_at"]
        author_id = self.github_dao.get_comment_user_id(event_created_at)
        if author_id is not None:
            issue_event["target_user_id"] = issue_event["creator_id"]
            issue_event["creator_id"] = author_id
            author_name = self.github_dao.get_user_name(author_id)
            issue_event["detail"] = author_name + " " + event_type + " " + event.actor.login + " in issue " + \
                                    str(issue_event["issue_id"])
        else:
            self.logger.warning(event.actor.login + " was mentioned in event created at " + str(event.created_at) +
                                " but no comment was found")
            issue_event["detail"] = event.actor.login + " " + event_type + " in issue " + str(issue_event["issue_id"])

    def __process_milestone_event(self, event, event_type, issue_event):
        issue_event["detail"] = event.actor.login + " " + event_type + " issue " + str(issue_event["issue_id"]) \
                                + " with " + event._rawData.get('milestone').get('title')

    def __process_commit_related_event(self, event, event_type, issue_event):
        self.__process_generic_event(event, event_type, issue_event)
        if event.commit_id is not None:
            issue_event["detail"] += " with commit " + event.commit_id
            commit_id = self.github_dao.get_commit_id(event.commit_id)
            if commit_id is not None:
                self.github_dao.insert_issue_commit(issue_event["issue_id"], commit_id)
            else:
                self.logger.warning("Issue " + str(issue_event["issue_id"]) + " is related to commit " +
                                    str(event.commit_id) + " but commit is not in db. Ignoring relationship.")
        else:
            issue_event["detail"] += " without commit"

    def __process_labeled_event(self, event, event_type, issue_event):
        issue_event["detail"] = event.actor.login + " " + event_type + " issue " + str(
            issue_event["issue_id"]) + " with " + event._rawData.get(
            'label').get('name')

    def __process_assigned_event(self, event, event_type, issue_event):
        assignee = event._rawData.get('assignee').get('login')
        issue_event["target_user_id"] = self.github_dao.get_user_id(assignee, None)
        issue_event["detail"] = event.actor.login + " " + event_type + " issue " + str(
            issue_event["issue_id"]) + " to " + event._rawData.get(
            'assignee').get('login')

    def __write_assignees(self, issue, issue_id):
        if issue.assignee is not None:
            assignee_id = self.github_dao.get_user_id(issue.assignee.login, issue.assignee.email)
            self.github_dao.insert_issue_assignee(issue_id, assignee_id)
