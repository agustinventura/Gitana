#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'agustin ventura'

import logging
import sys

sys.path.insert(0, "..\\..")
from github_dao import GithubDAO


class IssueWriter:
    def __init__(self, github_querier, issue_tracker_id, issues, config, issue_update):
        self.github_querier = github_querier
        self.github_dao = None
        self.config = config
        self.issue_tracker_id = issue_tracker_id
        self.issues = issues
        self.issue_update = issue_update

    def __del__(self):
        logging.debug("IssueWriter destroyed")
        if self.github_dao is not None:
            self.github_dao.close()

    def __call__(self):
        self.github_dao = GithubDAO(self.config)
        for issue in self.issues:
            if not self.issue_update:
                logging.info("Writing issue " + str(issue.number))
                self.write(issue)
            else:
                logging.info("Updating issue" + str(issue.number))
                self.update(issue)

    def write(self, issue):
        user_data = self.github_querier.read_user(issue)
        user_id = self.__write_user(user_data)
        issue_id = self.__write_issue(issue, user_id)
        self.__write_labels(issue, issue_id)
        self.__write_comments(issue, issue_id)
        self.__write_events(issue, issue_id)
        self.__write_assignees(issue, issue_id)

    def update(self, issue):
        user_data = self.github_querier.read_user(issue)
        user_id = self.__write_user(user_data)
        issue_id = self.__update_issue(issue, user_id)
        self.__update_labels(issue, issue_id)
        self.__update_comments(issue, issue_id)
        self.__write_events(issue, issue_id)
        self.__update_assignees(issue, issue_id)

    def __write_user(self, user_data):
        user_id = self.github_dao.get_user_id(user_data["login"], user_data["email"])
        return user_id

    def __write_issue(self, issue, user_id):
        issue_data = self.github_querier.read_issue(issue)
        issue_id = self.github_dao.insert_issue(user_id, issue_data["own_id"], issue_data["summary"],
                                                issue_data["version"], issue_data["created_at"],
                                                issue_data["updated_at"], self.issue_tracker_id)
        self.__write_issue_body(issue_id, user_id, issue_data)
        return issue_id

    def __update_issue(self, issue, user_id):
        issue_data = self.github_querier.read_issue(issue)
        issue_id = self.github_dao.get_issue_id_by_own_id(issue_data["own_id"], self.issue_tracker_id)
        self.github_dao.update_issue(issue_id, user_id, issue_data["summary"], issue_data["version"],
                                     issue_data["created_at"], issue_data["updated_at"])
        self.__update_issue_body(issue_id, user_id, issue_data)
        return issue_id

    def __write_labels(self, issue, issue_id):
        for label in self.github_querier.read_labels(issue):
            self.github_dao.insert_issue_label(issue_id, label)

    def __update_labels(self, issue, issue_id):
        self.github_dao.delete_issue_labels(issue_id)
        self.__write_labels(issue, issue_id)

    def __write_comments(self, issue, issue_id):
        comments = self.github_querier.read_comments(issue)
        for comment_pos, comment in enumerate(comments):
            comment_data = self.github_querier.read_comment(comment)
            user_data = self.github_querier.read_user_data(comment_data["user"])
            comment_data["user"] = self.__write_user(user_data)
            self.github_dao.insert_issue_comment(issue_id, comment_data["user"], comment_data["id"], comment_pos + 1,
                                                 comment_data["body"], comment_data["created_at"])

    def __update_comments(self, issue, issue_id):
        self.github_dao.delete_issue_comments(issue_id)
        self.__write_comments(issue, issue_id)

    def __write_events(self, issue, issue_id):
        for event in self.github_querier.read_events(issue):
            event_data = self.github_querier.read_event(event)
            if event_data["actor"] is not None:
                issue_event = {"issue_id": issue_id,
                               "issue_own_id": issue.number}
                actor_data = self.github_querier.read_actor(event)
                issue_event["creator_id"] = self.__write_user(actor_data)
                issue_event["event_type_id"] = self.github_dao.get_event_type_id(event_data["event"])
                issue_event["created_at"] = event_data["created_at"]
                issue_event["target_user_id"] = None
                self.__process_event(event_data, issue_event)
                self.github_dao.insert_issue_event(issue_event["issue_id"], issue_event["event_type_id"],
                                                   issue_event["detail"], issue_event["creator_id"],
                                                   issue_event["created_at"], issue_event["target_user_id"])
            else:
                logging.warning("Skipped event " + str(event.id) + " because it has no actor")

    def __process_event(self, event_data, issue_event):
        event_type = event_data["event"]
        if event_type in ["assigned", "unassigned"]:
            self.__process_assigned_event(event_data, event_type, issue_event)
        elif event_type in ["labeled", "unlabeled"]:
            self.__process_labeled_event(event_data, event_type, issue_event)
        elif event_type in ["closed", "merged", "referenced"]:
            self.__process_commit_related_event(event_data, event_type, issue_event)
        elif event_type in ["milestoned", "demilestoned"]:
            self.__process_milestone_event(event_data, event_type, issue_event)
        elif event_type == "mentioned":
            self.__process_mentioned_event(event_data, event_type, issue_event)
        elif event_type == "subscribed":
            self.__process_subscription_event(event_data, event_type, issue_event)
        else:
            self.__process_generic_event(event_data, event_type, issue_event)

    def __process_generic_event(self, event_data, event_type, issue_event):
        issue_event["detail"] = event_data["actor"].login + " " + event_type + " issue " + str(
            issue_event["issue_own_id"])

    def __process_subscription_event(self, event_data, event_type, issue_event):
        self.__process_generic_event(event_data, event_type, issue_event)
        self.github_dao.insert_issue_subscriber(issue_event["issue_id"], issue_event["creator_id"])

    def __process_mentioned_event(self, event_data, event_type, issue_event):
        event_created_at = issue_event["created_at"]
        author_id = self.github_dao.get_comment_user_id(event_created_at)
        if author_id is not None:
            issue_event["target_user_id"] = issue_event["creator_id"]
            issue_event["creator_id"] = author_id
            author_name = self.github_dao.get_user_name(author_id)
            issue_event["detail"] = author_name + " " + event_type + " " + event_data["actor"].login + " in issue " + \
                                    str(issue_event["issue_own_id"])
        else:
            logging.warning(
                event_data["actor"].login + " was mentioned in event created at " + str(event_data["created_at"]) +
                                " but no comment was found")
            issue_event["detail"] = event_data["actor"].login + " " + event_type + " in issue " + str(
                issue_event["issue_own_id"])

    def __process_milestone_event(self, event_data, event_type, issue_event):
        issue_event["detail"] = event_data["actor"].login + " " + event_type + " issue " + str(
            issue_event["issue_own_id"]) \
                                + " with " + event_data["milestone"]

    def __process_commit_related_event(self, event_data, event_type, issue_event):
        self.__process_generic_event(event_data, event_type, issue_event)
        if event_data["commit_id"] is not None:
            issue_event["detail"] += " with commit " + event_data["commit_id"]
            commit_id = self.github_dao.get_commit_id(event_data["commit_id"])
            if commit_id is not None:
                self.github_dao.insert_issue_commit(issue_event["issue_own_id"], commit_id)
            else:
                logging.warning("Issue " + str(issue_event["issue_own_id"]) + " is related to commit " +
                                str(event_data["commit_id"]) + " but commit is not in db. Ignoring relationship.")
        else:
            issue_event["detail"] += " without commit"

    def __process_labeled_event(self, event_data, event_type, issue_event):
        issue_event["detail"] = event_data["actor"].login + " " + event_type + " issue " + str(
            issue_event["issue_own_id"]) + " with " + event_data["label"]

    def __process_assigned_event(self, event_data, event_type, issue_event):
        assignee = event_data["assignee"]
        issue_event["target_user_id"] = self.github_dao.get_user_id(assignee, None)
        issue_event["detail"] = event_data["actor"].login + " " + event_type + " issue " + str(
            issue_event["issue_own_id"]) + " to " + str(event_data["assignee"])

    def __write_assignees(self, issue, issue_id):
        if issue.assignee is not None:
            assignee_id = self.github_dao.get_user_id(issue.assignee.login, issue.assignee.email)
            self.github_dao.insert_issue_assignee(issue_id, assignee_id)

    def __update_assignees(self, issue, issue_id):
        self.github_dao.delete_issue_assignee(issue_id)
        self.__write_assignees(issue, issue_id)

    def __write_issue_body(self, issue_id, user_id, issue_data):
        self.github_dao.insert_issue_comment(issue_id, user_id, 0, 0, issue_data["body"],
                                             issue_data["created_at"])
        if issue_data["body"] is not None:
            self.__write_issue_attachment(issue_data["body"], issue_id)

    def __update_issue_body(self, issue_id, user_id, issue_data):
        self.github_dao.delete_issue_comments(issue_id)
        self.github_dao.insert_issue_comment(issue_id, user_id, 0, 0, issue_data["body"],
                                             issue_data["created_at"])
        if issue_data["body"] is not None:
            self.github_dao.delete_issue_reference(issue_id)
            self.github_dao.delete_issue_attachment(issue_id)
            self.__write_issue_attachment(issue_data["body"], issue_id)

    def __write_issue_attachment(self, body, issue_id):
        attachment_urls = self.github_querier.read_issue_attachments(body)
        for attachment_url in attachment_urls:
            self.github_dao.insert_issue_attachment(issue_id, attachment_url)
