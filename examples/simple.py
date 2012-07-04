import sys, os
cwd = os.getcwd()
cwd = cwd[:cwd.rindex("/")]
sys.path.insert(0, cwd)

import pysonic
from pysonic import *

pysonic.config.setup(dict(
    default_db = ("localhost", "blog_test", "root"),
    default_rs = {"host" :"localhost"}
))

class Author(SonicObj):
    username = str_field("")
    nick = str_field("")
    intro = str_field("")

    uniques = {
        "username": [username]
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

Author.build_table()
Blog.build_table()


author = Author()
author.username = "admin"
author.nick = "Great Author"
author.intro = "I love blog"
#author.save()

blog = Blog()
blog.author = author
blog.title = "Hello world"
blog.content = "foo bar"
blog.tags = ["life", "testing"]
# blog.save()

for author in Author.walk_all_objs():
    print author.nick

author = Author.find_one(username = "admin")
print author
blogs = Blog.find_all_objs(author = author, tags = "life")
for blog in blogs:
    print blog
blogs = Blog.find_all_objs(author = author, tags = "testing")
for blog in blogs:
    print blog