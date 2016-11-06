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


def process_halflife(g):
    g.init_db("db_halflife")
    g.create_project("db_halflife", "halflife")
    g.import_git_data("db_halflife", "halflife", "halflife", "/home/agustin/Development/Python/Projects/halflife",
                      None, False, None, None)
    g.import_github_tracker_data("db_halflife", "halflife", "halflife", "https://github.com/ValveSoftware/halflife",
                                 "ValveSoftware/halflife", "493852caff7a314e894a4ea35002652de127b910")


def process_pruebagit(g):
    g.init_db("db_prueba")
    g.create_project("db_prueba", "prueba")
    g.import_git_data("db_prueba", "prueba", "prueba", "/home/agustin/Development/Python/Projects/PruebaGit",
                      None, False, None, None)
    g.import_github_tracker_data("db_prueba", "prueba", "prueba", "https://github.com/agustinventura/PruebaGit",
                                 "agustinventura/PruebaGit", "493852caff7a314e894a4ea35002652de127b910", False)


def update_pruebagit(g):
    g.update_github_tracker_data("db_prueba", "prueba", "prueba", "https://github.com/agustinventura/PruebaGit",
                                 "agustinventura/PruebaGit", "493852caff7a314e894a4ea35002652de127b910")


def main():
    g = Gitana(CONFIG, None)
    #process_pruebagit(g)
    update_pruebagit(g)
    #process_2048(g)
    # recover_2048(g)
    #process_halflife(g)

def process_2048(g):
    g.init_db("db_2048")
    g.create_project("db_2048", "2048")
    g.import_git_data("db_2048", "2048", "2048", "/home/agustin/Development/Python/Projects/2048",
                      None, False, None, None)
    g.import_github_tracker_data("db_2048", "2048", "2048", "https://github.com/gabrielecirulli/2048",
                                 "gabrielecirulli/2048", "493852caff7a314e894a4ea35002652de127b910", False)


def recover_2048(g):
    g.import_github_tracker_data("db_2048", "2048", "2048", "https://github.com/gabrielecirulli/2048",
                                 "gabrielecirulli/2048", "493852caff7a314e894a4ea35002652de127b910", True)

if __name__ == "__main__":
    main()
