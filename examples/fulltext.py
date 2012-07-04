import sys, os
cwd = os.getcwd()
cwd = cwd[:cwd.rindex("/")]
sys.path.insert(0, cwd)

import pysonic
from pysonic import *

# pysonic config must appear before Model declaration
pysonic.config.setup(dict(
    default_db = ("localhost", "blog_test", "root"),
    default_rs = {"host" :"localhost"},

    # must ensure testdb folder exist
    # debug also must be set to False to run fulltext search without separate deamon
    # code for fulltext search deamon will be release later
    xdb_path = "testdb",
    debug = False,
))

class Author(SonicObj):
    username = str_field("")
    nick = str_field("")
    intro = str_field("")

    uniques = {
        "username": [username]
    }

    fulltext_indexes = {
        nick : 10,
        intro : 1
    }

class Blog(SonicObj):
    title = str_field()
    content = str_field()
    author = obj_field(Author)
    tags = list_field(str)

    indexes = {
        "author": [author],
        "author_tags": [author, tags]
    }

# assume run simple.py to insert Author data
Author.rebuild_fulltext_indexes()
author_ids = Author.search(key = "love", pos = 0, count = 10)
authors = Author.get(author_ids)
for author in authors:
    print author
