#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'agustin ventura'

import re
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
        self.issue_reference_pattern = re.compile('\s#\d+\s')

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
        self.__write_issue_body(issue_id, user_id, issue.body, created_at)
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
        for comment_pos, comment in enumerate(issue.get_comments()):
            user_id = self.__write_user(comment.user)
            comment_id = comment.id
            comment_body = comment.body
            comment_created_at = self.date_util.get_timestamp(comment.created_at, "%Y-%m-%d %H:%M:%S")
            self.github_dao.insert_issue_comment(issue_id, user_id, comment_id, comment_pos + 1, comment_body,
                                                 comment_created_at)
            self.__write_issue_reference(comment_body, issue_id)

    def __write_events(self, issue, issue_id):
        for event in issue.get_events():
            if event.actor is not None:
                issue_event = {"issue_id": issue_id,
                               "issue_own_id": issue.number}
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
        if event_type in ["assigned", "unassigned"]:
            self.__process_assigned_event(event, event_type, issue_event)
        elif event_type in ["labeled", "unlabeled"]:
            self.__process_labeled_event(event, event_type, issue_event)
        elif event_type in ["closed", "merged", "referenced"]:
            self.__process_commit_related_event(event, event_type, issue_event)
        elif event_type in ["milestoned", "demilestoned"]:
            self.__process_milestone_event(event, event_type, issue_event)
        elif event_type == "mentioned":
            self.__process_mentioned_event(event, event_type, issue_event)
        elif event_type == "subscribed":
            self.__process_subscription_event(event, event_type, issue_event)
        else:
            self.__process_generic_event(event, event_type, issue_event)

    def __process_generic_event(self, event, event_type, issue_event):
        issue_event["detail"] = event.actor.login + " " + event_type + " issue " + str(issue_event["issue_own_id"])

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
                                    str(issue_event["issue_own_id"])
        else:
            self.logger.warning(event.actor.login + " was mentioned in event created at " + str(event.created_at) +
                                " but no comment was found")
            issue_event["detail"] = event.actor.login + " " + event_type + " in issue " + str(
                issue_event["issue_own_id"])

    def __process_milestone_event(self, event, event_type, issue_event):
        issue_event["detail"] = event.actor.login + " " + event_type + " issue " + str(issue_event["issue_own_id"]) \
                                + " with " + event._rawData.get('milestone').get('title')

    def __process_commit_related_event(self, event, event_type, issue_event):
        self.__process_generic_event(event, event_type, issue_event)
        if event.commit_id is not None:
            issue_event["detail"] += " with commit " + event.commit_id
            commit_id = self.github_dao.get_commit_id(event.commit_id)
            if commit_id is not None:
                self.github_dao.insert_issue_commit(issue_event["issue_own_id"], commit_id)
            else:
                self.logger.warning("Issue " + str(issue_event["issue_own_id"]) + " is related to commit " +
                                    str(event.commit_id) + " but commit is not in db. Ignoring relationship.")
        else:
            issue_event["detail"] += " without commit"

    def __process_labeled_event(self, event, event_type, issue_event):
        issue_event["detail"] = event.actor.login + " " + event_type + " issue " + str(
            issue_event["issue_own_id"]) + " with " + event._rawData.get(
            'label').get('name')

    def __process_assigned_event(self, event, event_type, issue_event):
        assignee = event._rawData.get('assignee').get('login')
        issue_event["target_user_id"] = self.github_dao.get_user_id(assignee, None)
        issue_event["detail"] = event.actor.login + " " + event_type + " issue " + str(
            issue_event["issue_own_id"]) + " to " + event._rawData.get(
            'assignee').get('login')

    def __write_assignees(self, issue, issue_id):
        if issue.assignee is not None:
            assignee_id = self.github_dao.get_user_id(issue.assignee.login, issue.assignee.email)
            self.github_dao.insert_issue_assignee(issue_id, assignee_id)

    def __write_issue_body(self, issue_id, user_id, body, created_at):
        self.github_dao.insert_issue_comment(issue_id, user_id, 0, 0, body,
                                             created_at)
        if body is not None:
            self.__write_issue_reference(body, issue_id)

    def __write_issue_reference(self, body, issue_id):
        issue_reference = self.issue_reference_pattern.search(body)
        if issue_reference is not None:
            referenced_issue_own_id = issue_reference.group()[2:]
            referenced_issue_id = self.github_dao.get_issue_id_by_own_id(referenced_issue_own_id);
            if referenced_issue_id is not None:
                self.github_dao.insert_issue_reference(issue_id, referenced_issue_id)
            else:
                self.logger.warning(
                    "Issue " + str(issue_id) + " references issue with own_id " + str(referenced_issue_own_id)
                    + " but no one exists")
