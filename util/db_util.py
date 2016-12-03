#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'valerio cosentino'

import mysql.connector


class DbUtil():
    def get_connection(self, config):
        return mysql.connector.connect(**config)

    def close_connection(self, cnx):
        cnx.close()

    def lowercase(self, str):
        if str:
            str = str.lower()

        return str

    def select_project_id(self, cnx, project_name, logger):
        found = None
        cursor = cnx.cursor()
        query = "SELECT p.id " \
                "FROM project p " \
                "WHERE p.name = %s"
        arguments = [project_name]
        cursor.execute(query, arguments)
        row = cursor.fetchone()
        cursor.close()

        if row:
            found = row[0]
        else:
            logger.error("the project " + str(project_name) + " does not exist")

        return found

    def insert_repo(self, cnx, project_id, repo_name, logger):
        cursor = cnx.cursor()
        query = "INSERT IGNORE INTO repository " \
                "VALUES (%s, %s, %s)"
        arguments = [None, project_id, repo_name]
        cursor.execute(query, arguments)
        cnx.commit()
        cursor.close()

    def insert_issue_tracker(self, cnx, repo_id, issue_tracker_name, type, logger):
        cursor = cnx.cursor()
        query = "INSERT IGNORE INTO issue_tracker " \
                "VALUES (%s, %s, %s, %s)"
        arguments = [None, repo_id, issue_tracker_name, type]
        cursor.execute(query, arguments)
        cnx.commit()

        query = "SELECT id " \
                "FROM issue_tracker " \
                "WHERE name = %s"
        arguments = [issue_tracker_name]
        cursor.execute(query, arguments)

        row = cursor.fetchone()
        cursor.close()

        if row:
            found = row[0]
        else:
            logger.warning("no issue with name " + str(issue_tracker_name))

        return found

    def select_repo_id(self, cnx, repo_name, logger):
        found = None
        cursor = cnx.cursor()
        query = "SELECT id " \
                "FROM repository " \
                "WHERE name = %s"
        arguments = [repo_name]
        cursor.execute(query, arguments)

        row = cursor.fetchone()
        cursor.close()

        if row:
            found = row[0]
        else:
            logger.error("the repository " + repo_name + " does not exist")

        return found

    def select_instant_messaging_id(self, cnx, im_name, logger):
        found = None
        cursor = cnx.cursor()
        query = "SELECT id " \
                "FROM instant_messaging " \
                "WHERE name = %s"
        arguments = [im_name]
        cursor.execute(query, arguments)

        row = cursor.fetchone()
        cursor.close()

        if row:
            found = row[0]
        else:
            logger.error("the instant messaging " + im_name + " does not exist")

        return found

    def insert_user(self, cnx, name, email, logger):
        cursor = cnx.cursor()

        query = "INSERT IGNORE INTO user " \
                "VALUES (%s, %s, %s)"
        arguments = [None, name, email]
        cursor.execute(query, arguments)
        cnx.commit()
        cursor.close()

    def select_user_id_by_email(self, cnx, email, logger):
        found = None
        if email:
            cursor = cnx.cursor()
            query = "SELECT id " \
                    "FROM user " \
                    "WHERE email = %s"
            arguments = [email]
            cursor.execute(query, arguments)

            row = cursor.fetchone()
            cursor.close()

            if row:
                found = row[0]
            else:
                logger.warning("there is not user with this email " + email)

        return found

    def select_user_id_by_name(self, cnx, name, logger):
        found = None
        if name:
            found = None
            cursor = cnx.cursor()
            query = "SELECT id " \
                    "FROM user " \
                    "WHERE name = %s"
            arguments = [name]
            cursor.execute(query, arguments)

            row = cursor.fetchone()
            cursor.close()

            if row:
                found = row[0]
            else:
                logger.warning("there is not user with this name " + name)

        return found

    def select_forum_id(self, cnx, forum_name, logger):
        found = None
        cursor = cnx.cursor()
        query = "SELECT id " \
                "FROM forum " \
                "WHERE name = %s"
        arguments = [forum_name]
        cursor.execute(query, arguments)

        row = cursor.fetchone()
        cursor.close()

        if row:
            found = row[0]
        else:
            logger.error("the forum " + forum_name + " does not exist")

        return found

    def select_issue_tracker_id(self, cnx, issue_tracker_name, logger):
        found = None
        cursor = cnx.cursor()
        query = "SELECT id " \
                "FROM issue_tracker " \
                "WHERE name = %s"
        arguments = [issue_tracker_name]
        cursor.execute(query, arguments)

        row = cursor.fetchone()
        cursor.close()

        if row:
            found = row[0]
        else:
            logger.error("the issue tracker " + issue_tracker_name + " does not exist")

        return found

    def get_issue_dependency_type_id(self, cnx, name):
        found = None
        cursor = cnx.cursor()
        query = "SELECT id FROM issue_dependency_type WHERE name = %s"
        arguments = [name]
        cursor.execute(query, arguments)

        row = cursor.fetchone()
        cursor.close()

        if row:
            found = row[0]

        return found

    def get_message_type_id(self, cnx, name):
        found = None
        cursor = cnx.cursor()
        query = "SELECT id FROM message_type WHERE name = %s"
        arguments = [name]
        cursor.execute(query, arguments)
        row = cursor.fetchone()
        cursor.close()

        if row:
            found = row[0]

        return found

    def restart_connection(self, config, logger):
        logger.info("restarting connection...")
        return mysql.connector.connect(**config)
