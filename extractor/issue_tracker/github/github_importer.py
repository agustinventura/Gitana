#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'agustin ventura'

import datetime
import multiprocessing
import sys

sys.path.insert(0, "..\\..")
from github_querier import GithubQuerier
from github_dao import GithubDAO
from issue_writer import IssueWriter
from extractor.util import multiprocessing_util

class GithubImporter:
    NUM_PROCESSES = 5

    def __init__(self, db_name, project_name, repo_name, url, github_repo_name, access_token, recover_import, config,
                 logger):
        self.logger = logger
        self.log_path = self.logger.name.rsplit('.', 1)[0] + "-" + project_name
        self.type = "github"
        self.url = url
        self.project_name = project_name
        self.db_name = db_name
        self.repo_name = repo_name
        self.repo_id = None
        self.github_repo_name = github_repo_name
        config.update({'database': db_name})
        self.config = config
        self.github_reader = GithubQuerier(self.github_repo_name, access_token, self.logger)
        self.github_dao = GithubDAO(config, self.logger)
        self.recover_import = recover_import
        self.processes = GithubImporter.NUM_PROCESSES
        self.access_token = access_token

    def import_issues(self):
        # 1) Init database
        self.repo_id = self.github_dao.get_repo_id(self.project_name, self.repo_name)
        issue_tracker_id = self.github_dao.get_issue_tracker_id(self.repo_id, self.url, self.type)
        if not self.recover_import:
            # 2.a) Read all the issues
            self.read_all_issues(issue_tracker_id)
            # else:
            # 2.b) Read only modified issues since last date in database
            #self.read_new_issues(issue_tracker_id, issue_writer)

    def read_all_issues(self, issue_tracker_id):
        pages = []
        issues_page = self.github_reader.load_issues_page(None)
        while issues_page is not None:
            pages.append(issues_page)
            issues_page = self.github_reader.load_issues_page(None)
        self.__write_issues_pages(pages, issue_tracker_id)

    def read_new_issues(self, issue_tracker_id, issue_writer):
        max_created_at = self.github_dao.get_issue_max_created_at(issue_tracker_id)
        max_updated_at = self.github_dao.get_issue_max_last_change_at(issue_tracker_id);
        max_date = None
        if max_created_at > max_updated_at:
            max_date = max_created_at
        else:
            max_date = max_updated_at
        max_date = max_date + datetime.timedelta(seconds=1)
        issues = self.github_reader.load_issues_page(max_date)
        while issues is not None:
            for issue in issues:
                # 3) Write the issue
                issue_writer.write(issue)
            issues = self.github_reader.load_issues_page(max_date)

    def __write_issues_pages(self, pages, issue_tracker_id):
        intervals = multiprocessing_util.get_tasks_intervals(pages, self.processes)
        queue_intervals = multiprocessing.JoinableQueue()
        results = multiprocessing.Queue()
        # Start consumers
        multiprocessing_util.start_consumers(self.processes, queue_intervals, results)
        for interval in intervals:
            github_reader = GithubQuerier(self.github_repo_name, self.access_token, self.logger)
            github_dao = GithubDAO(self.config, self.logger)
            issue_writer = IssueWriter(github_reader, github_dao, issue_tracker_id, interval, self.logger)
            queue_intervals.put(issue_writer)

            # Add end-of-queue markers
        multiprocessing_util.add_poison_pills(self.processes, queue_intervals)

        # Wait for all of the tasks to finish
        queue_intervals.join()
