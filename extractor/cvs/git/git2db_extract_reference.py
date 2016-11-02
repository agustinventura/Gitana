#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'valerio cosentino'

import re
from datetime import datetime
import logging
import logging.handlers
import sys
sys.path.insert(0, "..//..")

from querier_git import GitQuerier
from git_dao import GitDao

#do not import patches
LIGHT_IMPORT_TYPE = 1
#import patches but not at line level
MEDIUM_IMPORT_TYPE = 2
#import patches also at line level
FULL_IMPORT_TYPE = 3


class Git2DbReference(object):

    def __init__(self, db_name,
                 repo_id, git_repo_path, before_date, import_type, ref_name, from_sha,
                 config, log_path):
        self.log_path = log_path
        self.git_repo_path = git_repo_path
        self.repo_id = repo_id
        self.db_name = db_name
        self.ref_name = ref_name
        self.before_date = before_date
        self.import_type = import_type
        self.from_sha = from_sha

        config.update({'database': db_name})
        self.config = config

    def __call__(self):
        LOG_FILENAME = self.log_path + "-git2db"
        self.logger = logging.getLogger(LOG_FILENAME)
        fileHandler = logging.FileHandler(LOG_FILENAME + "-" + self.make_it_printable(self.ref_name) + ".log", mode='w')
        formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s", "%Y-%m-%d %H:%M:%S")

        fileHandler.setFormatter(formatter)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(fileHandler)

        try:
            self.querier = GitQuerier(self.git_repo_path, self.logger)
            self.dao = GitDao(self.config, self.logger)
            self.extract()
        except Exception, e:
            self.logger.error("Git2Db failed", exc_info=True)

    def make_it_printable(self, str):
        u = str.decode('utf-8', 'ignore').lower()
        return re.sub(r'(\W|\s)+', '-', u)

    def get_ext(self, str):
        file_name = str.split('/')[-1]
        ext = file_name.split('.')[-1]
        return ext

    #not used, future extension
    def get_type(self, str):
        type = "text"
        if str.startswith('Binary files'):
            type = "binary"
        return type

    def get_info_contribution_in_reference(self, reference_name, repo_id, from_sha):
        for reference in self.querier.get_references():
            if reference[0] == reference_name:
                ref_name = reference[0]
                ref_type = reference[1]

                if from_sha:
                    if self.before_date:
                        commits = self.querier.collect_all_commits_after_sha_before_date(reference_name, from_sha, self.before_date)
                    else:
                        commits = self.querier.collect_all_commits_after_sha(reference_name, from_sha)

                    self.analyse_commits(commits, reference_name, repo_id)
                else:
                    if self.before_date:
                        commits = self.querier.collect_all_commits_before_date(reference_name, self.before_date)
                    else:
                        commits = self.querier.collect_all_commits(reference_name)

                    self.analyse_reference(reference_name, ref_type, repo_id)
                    self.analyse_commits(commits, reference_name, repo_id)

                break

    def analyse_reference(self, ref_name, ref_type, repo_id):
        self.dao.insert_reference(repo_id, ref_name, ref_type)

    def get_diffs_from_commit(self, commit, files_in_commit):
        if self.import_type > LIGHT_IMPORT_TYPE:
            diffs = self.querier.get_diffs(commit, files_in_commit, True)
        else:
            diffs = self.querier.get_diffs(commit, files_in_commit, False)

        return diffs

    def analyse_commit(self, commit, ref_id, repo_id):
        message = self.querier.get_commit_property(commit, "message")
        author_name = self.querier.get_commit_property(commit, "author.name")
        author_email = self.querier.get_commit_property(commit, "author.email")
        committer_name = self.querier.get_commit_property(commit, "committer.name")
        committer_email = self.querier.get_commit_property(commit, "committer.email")
        size = self.querier.get_commit_property(commit, "size")
        sha = self.querier.get_commit_property(commit, "hexsha")
        authored_date = self.querier.get_commit_time(self.querier.get_commit_property(commit, "authored_date"))
        committed_date = self.querier.get_commit_time(self.querier.get_commit_property(commit, "committed_date"))
        #insert author
        author_id = self.dao.get_user_id(author_name, author_email)
        committer_id = self.dao.get_user_id(committer_name, committer_email)

        commit_found = self.dao.select_commit_id(sha, repo_id)

        if not commit_found:
            #insert commit
            self.dao.insert_commit(repo_id, sha, message, author_id, committer_id, authored_date, committed_date, size)
            #insert parents of the commit
            self.dao.insert_commit_parents(commit.parents, commit_found, sha, repo_id)
            #insert commits in reference
            self.dao.insert_commit_in_reference(repo_id, commit_found, ref_id)

            commit_id = self.dao.select_commit_id(sha, repo_id)
            commit_stats_files = commit.stats.files
            try:
                if self.querier.commit_has_no_parents(commit):
                    for diff in self.querier.get_diffs_no_parent_commit(commit):
                        file_path = diff[0]
                        ext = self.get_ext(file_path)

                        self.dao.insert_file(repo_id, file_path, ext)
                        file_id = self.dao.select_file_id(repo_id, file_path)

                        if self.import_type > LIGHT_IMPORT_TYPE:
                            patch_content = re.sub(r'^(\w|\W)*\n@@', '@@', diff[1])
                        else:
                            patch_content = None

                        stats = self.querier.get_stats_for_file(commit_stats_files, file_path)
                        status = self.querier.get_status_with_diff(stats, diff)

                        #insert file modification
                        self.dao.insert_file_modification(commit_id, file_id, status, stats[0], stats[1], stats[2], patch_content)

                        if self.import_type == FULL_IMPORT_TYPE:
                            file_modification_id = self.dao.select_file_modification_id(commit_id, file_id)
                            line_details = self.querier.get_line_details(patch_content, ext)
                            for line_detail in line_details:
                                self.dao.insert_line_details(file_modification_id, line_detail)
                else:
                    for diff in self.get_diffs_from_commit(commit, commit_stats_files.keys()):
                        self.dao.check_connection_alive()
                        if self.querier.is_renamed(diff):
                            file_previous = self.querier.get_rename_from(diff)
                            ext_previous = self.get_ext(file_previous)

                            file_current = self.querier.get_file_current(diff)
                            ext_current = self.get_ext(file_current)

                            #insert new file
                            self.dao.insert_file(repo_id, file_current, ext_current)

                            #get id new file
                            current_file_id = self.dao.select_file_id(repo_id, file_current)

                            #retrieve the id of the previous file
                            previous_file_id = self.dao.select_file_id(repo_id, file_previous)

                            if not previous_file_id:
                                self.dao.insert_file(repo_id, file_previous, ext_previous)
                                previous_file_id = self.dao.select_file_id(repo_id, file_previous)

                            if current_file_id == previous_file_id:
                                self.logger.warning("previous file id is equal to current file id (" + str(current_file_id) + ") " + str(sha))
                            else:
                                self.dao.insert_file_renamed(repo_id, current_file_id, previous_file_id)
                            self.dao.insert_file_modification(commit_id, current_file_id, "renamed", 0, 0, 0, None)
                        else:
                            #insert file
                            #if the file does not have a path, it won't be inserted
                            try:
                                file_path = self.querier.get_file_path(diff)

                                ext = self.get_ext(file_path)

                                stats = self.querier.get_stats_for_file(commit_stats_files, file_path)
                                status = self.querier.get_status_with_diff(stats, diff)

                                #if the file is new, add it
                                if self.querier.is_new_file(diff):
                                    self.dao.insert_file(repo_id, file_path, ext)
                                file_id = self.dao.select_file_id(repo_id, file_path)

                                if not file_id:
                                    self.dao.insert_file(repo_id, file_path, ext)
                                    file_id = self.dao.select_file_id(repo_id, file_path)

                                if self.import_type > LIGHT_IMPORT_TYPE:
                                    #insert file modification (additions, deletions)
                                    patch_content = self.querier.get_patch_content(diff)
                                else:
                                    patch_content = None

                                self.dao.insert_file_modification(commit_id, file_id, status, stats[0], stats[1], stats[2], patch_content)

                                if self.import_type == FULL_IMPORT_TYPE:
                                    file_modification_id = self.dao.select_file_modification_id(commit_id, file_id)
                                    line_details = self.querier.get_line_details(patch_content, ext)
                                    for line_detail in line_details:
                                        self.dao.insert_line_details(file_modification_id, line_detail)
                            except Exception, e:
                                self.logger.error("Something went wrong with commit " + str(sha), exc_info=True)
            except Exception, e:
                self.logger.error("Git2Db failed on commit " + str(sha), exc_info=True)

        else:
            #insert parents of the commit
            self.dao.insert_commit_parents(commit.parents, commit_found, sha, repo_id)
            #insert commits in reference
            self.dao.insert_commit_in_reference(repo_id, commit_found, ref_id)

    def analyse_commits(self, commits, ref, repo_id):
        ref_id = self.dao.select_reference_id(repo_id, ref)

        for c in commits:
            #self.logger.info("analysing commit " + str(commits.index(c)+1) + "/" + str(len(commits)))
            self.analyse_commit(c, ref_id, repo_id)

    def extract(self):
        try:
            start_time = datetime.now()
            self.get_info_contribution_in_reference(self.ref_name, self.repo_id, self.from_sha)
            end_time = datetime.now()
            self.dao.close_connection()

            minutes_and_seconds = divmod((end_time-start_time).total_seconds(), 60)
            self.logger.info("Git2DbReference finished after " + str(minutes_and_seconds[0])
                         + " minutes and " + str(round(minutes_and_seconds[1], 1)) + " secs")
        except Exception, e:
            self.logger.error("Git2DbReference failed", exc_info=True)