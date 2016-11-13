#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'agustin ventura'

import multiprocessing
import sys

sys.path.insert(0, "..\\..")
sys.path.insert(0, "..\\..")

from github_querier import GithubQuerier
from github_dao import GithubDAO
from issue_writer import IssueWriter
from reference_writer import ReferenceWriter
from extractor.util import multiprocessing_util


class GithubUpdater:
    NUM_PROCESSES = 5

    def __init__(self, db_name, project_name, repo_name, url, github_repo_name, access_token, processes, config):
        self.type = "github"
        self.project_name = project_name
        self.db_name = db_name
        self.repo_name = repo_name
        self.url = url
        self.repo_id = None
        self.github_repo_name = github_repo_name
        config.update({'database': db_name})
        self.config = config
        self.github_reader = GithubQuerier(self.github_repo_name, access_token)
        self.github_dao = GithubDAO(config)
        if processes is None:
            self.processes = GithubUpdater.NUM_PROCESSES
        else:
            self.processes = processes
        self.access_token = access_token

    def update_issues(self):
        # 1) Init database
        self.repo_id = self.github_dao.get_repo_id(self.project_name, self.repo_name)
        issue_tracker_id = self.github_dao.get_issue_tracker_id(self.repo_id, self.url, self.type)
        # 2) Update all the issues
        own_ids = self.github_dao.get_own_ids(issue_tracker_id)
        issues = []
        for own_id in own_ids:
            issues.append(self.github_reader.load_issue(int(own_id)))
        # 3) Write the issues
        self.__write_issues(issues, issue_tracker_id)
        # 4) Build issue references
        self.__write_issue_references(issue_tracker_id)

    def __write_issues(self, issues, issue_tracker_id):
        intervals = multiprocessing_util.get_tasks_intervals(issues, self.processes)
        queue_intervals = multiprocessing.JoinableQueue()
        results = multiprocessing.Queue()
        # Start consumers
        multiprocessing_util.start_consumers(self.processes, queue_intervals, results)
        for interval in intervals:
            github_reader = GithubQuerier(self.github_repo_name, self.access_token)
            issue_writer = IssueWriter(github_reader, issue_tracker_id, interval, self.config, True)
            queue_intervals.put(issue_writer)

        # Add end-of-queue markers
        multiprocessing_util.add_poison_pills(self.processes, queue_intervals)
        # Wait for all of the tasks to finish
        queue_intervals.join()

    def __write_issue_references(self, issue_tracker_id):
        comments = self.github_dao.get_issue_comments(issue_tracker_id)
        intervals = multiprocessing_util.get_tasks_intervals(comments, self.processes)
        queue_intervals = multiprocessing.JoinableQueue()
        results = multiprocessing.Queue()
        # Start consumers
        multiprocessing_util.start_consumers(self.processes, queue_intervals, results)
        for interval in intervals:
            github_reader = GithubQuerier(self.github_repo_name, self.access_token)
            reference_writer = ReferenceWriter(issue_tracker_id, interval, self.config, github_reader)
            queue_intervals.put(reference_writer)

        # Add end-of-queue markers
        multiprocessing_util.add_poison_pills(self.processes, queue_intervals)
        # Wait for all of the tasks to finish
        queue_intervals.join()
