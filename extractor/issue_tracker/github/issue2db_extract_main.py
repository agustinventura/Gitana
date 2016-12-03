#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time

from github import GithubException

from util import multiprocessing_util

__author__ = 'agustin ventura'

import logging
import multiprocessing
import sys

sys.path.insert(0, "..\\..")
from querier_github import GithubQuerier
from github_dao import GithubDAO
from issue2db_extract_issue import GithubIssue2Db
from issue2db_extract_issue_dependency import GithubIssueDependency2Db
from querier_github import IssuePageReader


class GitHubIssue2DbMain:
    NUM_PROCESSES = 5

    def __init__(self, db_name, project_name, repo_name, tracker_name, github_repo_name, access_tokens, processes,
                 config):
        self.type = "github"
        self.tracker_name = tracker_name
        self.project_name = project_name
        self.db_name = db_name
        self.repo_name = repo_name
        self.repo_id = None
        self.github_repo_name = github_repo_name
        config.update({'database': db_name})
        self.config = config
        self.github_reader = GithubQuerier(self.github_repo_name, access_tokens[0])
        self.github_dao = None
        if processes is None:
            self.processes = GitHubIssue2DbMain.NUM_PROCESSES
        else:
            self.processes = processes
        self.access_tokens = access_tokens

    def extract(self):
        # 1) Init database
        self.github_dao = GithubDAO(self.config)
        logging.info("Initializing issues database")
        self.repo_id = self.github_dao.get_repo_id(self.project_name, self.repo_name)
        issue_tracker_id = self.github_dao.get_issue_tracker_id(self.repo_id, self.tracker_name, self.type)
        issues = self.read_new_issues(issue_tracker_id)
        self.github_dao.close()
        self.github_dao = None
        # 3) Write the issues
        logging.info("Writing issues")
        self.__write_issues(issues, issue_tracker_id)
        # 4) Build issue references
        logging.info("Writing issues references")
        self.__write_issue_references(issue_tracker_id)

    def read_all_issues(self):
        page_number = self.get_pages()
        logging.info(
            "Reading " + str(page_number) + " pages of issues using " + str(len(self.access_tokens)) + " tokens")
        results = None
        if page_number >= 0:
            pages = range(0, page_number)
            if not pages:
                pages = [0]
            pages_intervals = multiprocessing_util.get_tasks_intervals(pages, len(self.access_tokens))
            number_of_consumers = len(pages_intervals)
            queue_intervals = multiprocessing.JoinableQueue()
            results = multiprocessing.Queue()
            multiprocessing_util.start_consumers(number_of_consumers, queue_intervals, results)
            logging.info("Reading pages")
            for i, page in enumerate(pages_intervals):
                page_reader = IssuePageReader(self.access_tokens[i], self.github_repo_name, page)
                queue_intervals.put(page_reader)
            multiprocessing_util.add_poison_pills(number_of_consumers, queue_intervals)
            logging.info("Waiting for page readers to finish")
            queue_intervals.join()
        issues = []
        if results is not None:
            for i in range(0, number_of_consumers):
                result = results.get()
                for issues_page in result:
                    issues += issues_page
        logging.info("Read " + str(len(issues)) + " issues")
        return issues

    def get_pages(self):
        page_number = None
        while page_number is None:
            try:
                page_number = self.github_reader.get_last_page()
            except GithubException as e:
                if e.status == 403:
                    logging.info(
                        "Exceded token capacity while reading pages. Awaiting one hour")
                    time.sleep(3600)
                    logging.info("Restarting page reading")
                else:
                    logging.error("Caught unknown exception while reading pages: " + str(e))
            except Exception as e:
                logging.error("Caught unknown exception while reading pages: " + str(e))
        return page_number

    def read_new_issues(self, issue_tracker_id):
        issues = []
        all_issues = self.read_all_issues()
        logging.info("Read " + str(len(all_issues)) + " issues")
        issues_in_db = self.github_dao.get_own_ids(issue_tracker_id)
        logging.info("There are " + str(len(issues_in_db)) + " issues in database")
        for issue in all_issues:
            if str(issue.number) not in issues_in_db:
                issues.append(issue)
        logging.info("Found " + str(len(issues)) + " new issues")
        return issues

    def __write_issues(self, issues, issue_tracker_id):
        logging.info("Writing issues " + str(len(issues)) + " using " + str(self.processes) + " processes")
        intervals = multiprocessing_util.get_tasks_intervals(issues, self.processes)
        number_of_writers = len(intervals)
        queue_intervals = multiprocessing.JoinableQueue()
        results = multiprocessing.Queue()
        multiprocessing_util.start_consumers(number_of_writers, queue_intervals, results)
        logging.info("Starting writers")
        for interval in intervals:
            github_reader = GithubQuerier(self.github_repo_name, self.access_tokens[0])
            issue_writer = GithubIssue2Db(github_reader, issue_tracker_id, interval, self.config)
            queue_intervals.put(issue_writer)
        multiprocessing_util.add_poison_pills(number_of_writers, queue_intervals)
        logging.info("Waiting for writers to finish")
        queue_intervals.join()

    def __write_issue_references(self, issue_tracker_id):
        self.github_dao = GithubDAO(self.config)
        comments = self.github_dao.get_issue_comments(issue_tracker_id)
        logging.info("Analyzing " + str(len(comments)) + " comments using " + str(self.processes) + " processes")
        intervals = multiprocessing_util.get_tasks_intervals(comments, self.processes)
        number_of_writers = len(intervals)
        queue_intervals = multiprocessing.JoinableQueue()
        results = multiprocessing.Queue()
        # Start consumers
        multiprocessing_util.start_consumers(number_of_writers, queue_intervals, results)
        logging.info("Starting analyzers")
        for interval in intervals:
            github_reader = GithubQuerier(self.github_repo_name, self.access_tokens[0])
            reference_writer = GithubIssueDependency2Db(issue_tracker_id, interval, self.config, github_reader)
            queue_intervals.put(reference_writer)
        # Add end-of-queue markers
        multiprocessing_util.add_poison_pills(number_of_writers, queue_intervals)
        # Wait for all of the tasks to finish
        logging.info("Waiting for analyzers to finish")
        queue_intervals.join()
