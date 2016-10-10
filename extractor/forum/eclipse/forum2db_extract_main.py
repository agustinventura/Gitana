#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'valerio cosentino'

import mysql.connector
from mysql.connector import errorcode
from datetime import datetime
import multiprocessing
import sys
sys.path.insert(0, "..//..//..")

from querier_eclipse_forum import EclipseForumQuerier
from forum2db_extract_topic import Topic2Db
from extractor.util import consumer


class Forum2DbMain():

    def __init__(self, db_name, project_name,
                 type, url, before_date, recover_import, num_processes,
                 config, logger):
        self.logger = logger
        self.log_path = self.logger.name.rsplit('.', 1)[0] + "-" + project_name
        self.type = type
        self.url = url
        self.project_name = project_name
        self.db_name = db_name
        self.before_date = before_date
        self.recover_import = recover_import
        self.num_processes = num_processes

        config.update({'database': db_name})
        self.config = config

        try:
            self.querier = EclipseForumQuerier(self.url, self.logger)
            self.cnx = mysql.connector.connect(**self.config)
        except:
            self.logger.error("Forum2Db extract failed", exc_info=True)

    def select_project_id(self):
        found = None
        cursor = self.cnx.cursor()
        query = "SELECT p.id " \
                "FROM project p " \
                "WHERE p.name = %s"
        arguments = [self.project_name]
        cursor.execute(query, arguments)
        row = cursor.fetchone()
        cursor.close()

        if row:
            found = row[0]
        else:
            self.logger.error("the project " + self.project_name + " does not exist")

        return found

    def insert_forum(self, project_id):
        cursor = self.cnx.cursor()
        query = "INSERT IGNORE INTO forum " \
                "VALUES (%s, %s, %s, %s)"
        arguments = [None, project_id, self.url, self.type]
        cursor.execute(query, arguments)
        self.cnx.commit()

        query = "SELECT id " \
                "FROM forum " \
                "WHERE url = %s"
        arguments = [self.url]
        cursor.execute(query, arguments)

        row = cursor.fetchone()
        cursor.close()

        if row:
            found = row[0]
        else:
            self.logger("no forum linked to " + str(self.url))

        return found

    def get_intervals(self, elements):
        elements.sort()
        chunk_size = len(elements)/self.num_processes

        if chunk_size == 0:
            chunks = [elements]
        else:
            chunks = [elements[i:i + chunk_size] for i in range(0, len(elements), chunk_size)]

        intervals = []
        for chunk in chunks:
            intervals.append(chunk)

        return intervals

    def select_topic_id(self, forum_id, own_id):
        try:
            cursor = self.cnx.cursor()
            query = "SELECT id FROM topic WHERE forum_id = %s AND own_id = %s"
            arguments = [forum_id, own_id]
            cursor.execute(query, arguments)

            row = cursor.fetchone()
            cursor.close()
            if row:
                found = row[0]

            return found
        except Exception, e:
            self.logger.warning("topic id " + str(own_id) + " not found for forum id: " + str(forum_id), exc_info=True)

    def insert_topic(self, own_id, forum_id, title, views):
        try:
            cursor = self.cnx.cursor()
            query = "INSERT IGNORE INTO topic " \
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            arguments = [None, own_id, forum_id, title.lower(), None, views, None, None]
            cursor.execute(query, arguments)
            self.cnx.commit()
            cursor.close()
        except Exception, e:
            self.logger.warning("topic with title " + title.lower() + " not inserted for forum id: " + str(forum_id), exc_info=True)

    def get_topic_info(self, forum_id, topic):
        own_id = self.querier.get_topic_own_id(topic)
        title = self.querier.get_topic_title(topic)
        views = self.querier.get_topic_views(topic)

        self.insert_topic(own_id, forum_id, title, views)
        return self.select_topic_id(forum_id, own_id)

    def get_topic_ids(self, forum_id):
        topic_ids = []

        next_page = True
        while next_page:
            topics_on_page = self.querier.get_topics()

            for t in topics_on_page:
                topic_id = self.get_topic_info(forum_id, t)
                topic_ids.append(topic_id)

            next_page = self.querier.go_next_page()

        return topic_ids

    def get_topics(self, forum_id):
        self.querier.start_browser()
        topic_ids = self.get_topic_ids(forum_id)
        self.querier.close_browser()

        intervals = [i for i in self.get_intervals(topic_ids) if len(i) > 0]

        queue_extractors = multiprocessing.JoinableQueue()
        results = multiprocessing.Queue()

        # Start consumers
        consumer.start_consumers(self.num_processes, queue_extractors, results)

        for interval in intervals:
            topic_extractor = Topic2Db(self.db_name, forum_id, interval, self.config, self.log_path)
            queue_extractors.put(topic_extractor)

        # Add end-of-queue markers
        consumer.add_poison_pills(self.num_processes, queue_extractors)

        # Wait for all of the tasks to finish
        queue_extractors.join()

    def extract(self):
        try:
            start_time = datetime.now()
            project_id = self.select_project_id()
            forum_id = self.insert_forum(project_id)
            self.get_topics(forum_id)
            self.cnx.close()
            end_time = datetime.now()

            minutes_and_seconds = divmod((end_time-start_time).total_seconds(), 60)
            self.logger.info("Forum2Db extract finished after " + str(minutes_and_seconds[0])
                         + " minutes and " + str(round(minutes_and_seconds[1], 1)) + " secs")
        except:
            self.logger.error("Forum2Db extract failed", exc_info=True)