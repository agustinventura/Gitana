#!/usr/bin/env python
# -*- coding: utf-8 -*-
from gitana.gitana import Gitana

__author__ = 'valerio cosentino'

CONFIG = {
    'user': 'gitana',
    'password': 'gitana',
    'host': '192.168.56.101',
    'port': '3306',
    'raise_on_warnings': False,
    'buffered': True
}


def test_1(g):
    g.init_db("db_prueba")
    g.create_project("db_prueba", "prueba")
    g.import_git_data("db_prueba", "prueba", "prueba", "/home/agustin/Development/Python/Projects/PruebaGit",
                      None, False, None, 3)
    print "Importing small project using one process"
    g.import_github_tracker_data("db_prueba", "prueba", "prueba", "github_pruebagit",
                                 "agustinventura/PruebaGit", [], 1)


def test_2(g):
    print "Updating small project using one process"
    g.update_github_tracker_data("db_prueba", "prueba", "prueba", "github_pruebagit",
                                 "agustinventura/PruebaGit", [], 1)


def test_3(g):
    g.init_db("db_2048")
    g.create_project("db_2048", "2048")
    g.import_git_data("db_2048", "2048", "2048", "/home/agustin/Development/Python/Projects/2048",
                      None, False, None, 5)
    print "Importing medium project using default processes"
    g.import_github_tracker_data("db_2048", "2048", "2048", "github_2048",
                                 "gabrielecirulli/2048", [], None)


def test_4(g):
    print "Updating small project using default processes"
    g.update_github_tracker_data("db_2048", "2048", "2048", "github_2048",
                                 "gabrielecirulli/2048", [], None)


def test_5(g):
    g.init_db("db_halflife")
    g.create_project("db_halflife", "halflife")
    g.import_git_data("db_halflife", "halflife", "halflife", "/home/agustin/Development/Python/Projects/halflife",
                      None, False, None, 10)
    print "Importing big project using ten processes"
    g.import_github_tracker_data("db_halflife", "halflife", "halflife", "github_halflife",
                                 "ValveSoftware/halflife", [], 10)


def test_6(g):
    print "Updating big project using ten processes"
    g.update_github_tracker_data("db_halflife", "halflife", "halflife", "github_halflife",
                                 "ValveSoftware/halflife", [], 10)


def main():
    g = Gitana(CONFIG, None)
    g.delete_previous_logs()

    print "starting 1.."
    test_1(g)
    print "starting 2.."
    test_2(g)


if __name__ == "__main__":
    main()
