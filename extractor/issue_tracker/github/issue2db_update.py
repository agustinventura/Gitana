#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'agustin ventura'

import logging
import multiprocessing
import sys

sys.path.insert(0, "..\\..")
sys.path.insert(0, "..\\..")

from querier_github import GithubQuerier
from querier_github import IssueReader
from github_dao import GithubDAO
from issue2db_extract_issue import GithubIssue2Db
from issue2db_extract_issue_dependency import GithubIssueDependency2Db
from extractor.util import multiprocessing_util


class GithubIssue2DbUpdate:
    NUM_PROCESSES = 5

    def __init__(self, db_name, project_name, repo_name, tracker_name, github_repo_name, access_tokens, processes,
                 config):
        self.type = "github"
        self.project_name = project_name
        self.db_name = db_name
        self.repo_name = repo_name
        self.tracker_name = tracker_name
        self.repo_id = None
        self.github_repo_name = github_repo_name
        config.update({'database': db_name})
        self.config = config
        self.github_reader = GithubQuerier(self.github_repo_name, access_tokens[0])
        self.github_dao = GithubDAO(config)
        if processes is None:
            self.processes = GithubIssue2DbUpdate.NUM_PROCESSES
        else:
            self.processes = processes
        self.access_tokens = access_tokens

    def update_issues(self):
        # 1) Init database
        self.repo_id = self.github_dao.get_repo_id(self.project_name, self.repo_name)
        issue_tracker_id = self.github_dao.get_issue_tracker_id(self.repo_id, self.tracker_name, self.type)
        # 2) Update all the issues
        own_ids = self.github_dao.get_own_ids(issue_tracker_id)
        logging.info("updating " + str(len(own_ids)) + " issues")
        issues = self.__read_issues(own_ids)
        # 3) Write the issues
        self.__write_issues(issues, issue_tracker_id)
        # 4) Build issue references
        self.__write_issue_references(issue_tracker_id)

    def __read_issues(self, own_ids):
        logging.info("Reading " + str(len(own_ids)) + " issues using " + str(len(self.access_tokens)) + " tokens")
        issues_intervals = multiprocessing_util.get_tasks_intervals(own_ids, len(self.access_tokens))
        number_of_consumers = len(issues_intervals)
        queue_intervals = multiprocessing.JoinableQueue()
        results = multiprocessing.Queue()
        multiprocessing_util.start_consumers(number_of_consumers, queue_intervals, results)
        logging.info("Reading issues")
        for i, page in enumerate(issues_intervals):
            page_reader = IssueReader(self.access_tokens[i], self.github_repo_name, page)
            queue_intervals.put(page_reader)
        multiprocessing_util.add_poison_pills(number_of_consumers, queue_intervals)
        logging.info("Waiting for issue readers to finish")
        queue_intervals.join()
        issues = []
        if results is not None:
            for i in range(0, number_of_consumers):
                result = results.get()
                issues += result
        logging.info("Read " + str(len(issues)) + " issues")
        return issues

    def __write_issues(self, issues, issue_tracker_id):
        logging.info("Writing issues " + str(len(issues)) + " using " + str(self.processes) + " threads")
        intervals = multiprocessing_util.get_tasks_intervals(issues, self.processes)
        number_of_consumers = len(intervals)
        queue_intervals = multiprocessing.JoinableQueue()
        results = multiprocessing.Queue()
        logging.info("Starting writers")
        multiprocessing_util.start_consumers(number_of_consumers, queue_intervals, results)
        for interval in intervals:
            github_reader = GithubQuerier(self.github_repo_name, self.access_tokens[0])
            issue_writer = GithubIssue2Db(github_reader, issue_tracker_id, interval, self.config)
            queue_intervals.put(issue_writer)
        multiprocessing_util.add_poison_pills(number_of_consumers, queue_intervals)
        logging.info("Waiting for writers to finish")
        queue_intervals.join()

    def __write_issue_references(self, issue_tracker_id):
        comments = self.github_dao.get_issue_comments(issue_tracker_id)
        logging.info("Analyzing " + str(len(comments)) + " comments using " + str(self.processes) + " threads")
        intervals = multiprocessing_util.get_tasks_intervals(comments, self.processes)
        number_of_writers = len(intervals)
        queue_intervals = multiprocessing.JoinableQueue()
        results = multiprocessing.Queue()
        multiprocessing_util.start_consumers(number_of_writers, queue_intervals, results)
        logging.info("Starting analyzers")
        for interval in intervals:
            github_reader = GithubQuerier(self.github_repo_name, self.access_tokens[0])
            reference_writer = GithubIssueDependency2Db(issue_tracker_id, interval, self.config, github_reader)
            queue_intervals.put(reference_writer)
        multiprocessing_util.add_poison_pills(number_of_writers, queue_intervals)
        logging.info("Waiting for analyzers to finish")
        queue_intervals.join()
