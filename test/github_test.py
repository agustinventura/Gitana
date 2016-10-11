#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'valerio cosentino'

from gitana import Gitana

CONFIG = {
    'user': 'gitana',
    'password': 'gitana',
    'host': '192.168.56.101',
    'port': '3306',
    'raise_on_warnings': False,
    'buffered': True
}


def main():
    g = Gitana(CONFIG, None)
    g.create_project("db_2048", "2048")
    g.import_git_data("db_2048", "2048", "2048", "/home/agustin/Development/Python/Projects/2048",
                      None, False, None, None)
    g.import_github_tracker_data("db_2048", "2048", "2048", "https://github.com/gabrielecirulli/2048",
                                 "gabrielecirulli/2048")


if __name__ == "__main__":
    main()
