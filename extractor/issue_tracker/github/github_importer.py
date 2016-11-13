#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'agustin ventura'

import datetime
import logging
import multiprocessing
import sys

sys.path.insert(0, "..\\..")
from github_querier import GithubQuerier
from github_dao import GithubDAO
from issue_writer import IssueWriter
from reference_writer import ReferenceWriter
from extractor.util import multiprocessing_util


class GithubImporter:
    NUM_PROCESSES = 5

    def __init__(self, db_name, project_name, repo_name, url, github_repo_name, access_token, recover_import, config):
        self.type = "github"
        self.url = url
        self.project_name = project_name
        self.db_name = db_name
        self.repo_name = repo_name
        self.repo_id = None
        self.github_repo_name = github_repo_name
        config.update({'database': db_name})
        self.config = config
        self.github_reader = GithubQuerier(self.github_repo_name, access_token)
        self.github_dao = GithubDAO(config)
        self.recover_import = recover_import
        self.processes = GithubImporter.NUM_PROCESSES
        self.access_token = access_token

    def import_issues(self):
        # 1) Init database
        logging.info("Initializing issues database")
        self.repo_id = self.github_dao.get_repo_id(self.project_name, self.repo_name)
        issue_tracker_id = self.github_dao.get_issue_tracker_id(self.repo_id, self.url, self.type)
        issues = None
        if not self.recover_import:
            # 2.a) Read all the issues
            logging.info("Reading all the issues")
            issues = self.read_all_issues()
        else:
            # 2.b) Read only modified issues since last date in database
            logging.info("Recovering import")
            issues = self.read_new_issues(issue_tracker_id)
        # 3) Write the issues
        logging.info("Writing issues")
        self.__write_issues(issues, issue_tracker_id)
        # 4) Build issue references
        logging.info("Writing issues references")
        self.__write_issue_references(issue_tracker_id)

    def read_all_issues(self):
        issues = []
        issues_page = self.github_reader.load_issues_page(None)
        while issues_page is not None:
            issues += issues_page
            issues_page = self.github_reader.load_issues_page(None)
        return issues

    def read_new_issues(self, issue_tracker_id):
        max_created_at = self.github_dao.get_issue_max_created_at(issue_tracker_id)
        max_updated_at = self.github_dao.get_issue_max_last_change_at(issue_tracker_id);
        max_date = None
        if max_created_at > max_updated_at:
            max_date = max_created_at
        else:
            max_date = max_updated_at
        max_date = max_date + datetime.timedelta(seconds=1)
        logging.info("Reading new issues since " + str(max_date))
        issues = []
        issues_page = self.github_reader.load_issues_page(max_date)
        while issues_page is not None:
            issues += issues_page
            issues_page = self.github_reader.load_issues_page(max_date)
        return issues

    def __write_issues(self, issues, issue_tracker_id):
        logging.info("Writing issues using " + str(self.processes) + " threads")
        intervals = self.__divide_elements(issues)
        queue_intervals = multiprocessing.JoinableQueue()
        results = multiprocessing.Queue()
        # Start consumers
        multiprocessing_util.start_consumers(self.processes, queue_intervals, results)
        logging.info("Starting writers")
        for interval in intervals:
            github_reader = GithubQuerier(self.github_repo_name, self.access_token)
            issue_writer = IssueWriter(github_reader, issue_tracker_id, interval, self.config, False)
            queue_intervals.put(issue_writer)

        # Add end-of-queue markers
        multiprocessing_util.add_poison_pills(self.processes, queue_intervals)
        # Wait for all of the tasks to finish
        logging.info("Waiting for writers to finish")
        queue_intervals.join()

    def __divide_elements(self, elements):
        elements_length = len(elements)
        if elements_length < self.processes:
            return [elements]
        else:
            sublist_size = elements_length / self.processes
            issues_by_process = []
            for i in range(0, elements_length, sublist_size):
                issue_range = elements[i:i + sublist_size]
                issues_by_process.append(issue_range)
            return issues_by_process

    def __write_issue_references(self, issue_tracker_id):
        self.github_dao = GithubDAO(self.config)
        comments = self.github_dao.get_issue_comments(issue_tracker_id)
        intervals = self.__divide_elements(comments)
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
