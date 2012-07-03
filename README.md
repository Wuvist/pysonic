PySonic
=======
PySonic is an ORM layer for python using Redis/MySQL backend.

The idea is to store serilized data in MySQL, and use Redis for index storage, thus to achieve schemaless design and better performance.

It currently design for small to medium size database project which may afford putting all index in memory(Redis).

Design Goals
============
* Store serilized data in MySQL
* Store all index in Redis
* ActiveRecord Pattern API
* Schemaless design
* Optional fulltext search
* Auto admin website as plug-in
* Sharding support for large database

Dependencies
============
* easy_install python-mysql
* easy_install redis
* easy_install python-xapian (only needed if fulltext support required)

Example
=======

Model Definition
----------------
```python
class Author(SonicObj):
  username = str_field("")
    nick = str_field("")
    intro = str_field("")

    fulltext_indexes = {
	    nick : 10,
	    intro : 1
    }

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
        "author_tags": [author, tags],
    }
```

Build Table
-----------
```python
import pysonic
pysonic.config.setup(dict(
	default_db = ("localhost", "blog_db", "root"),
	default_rs = {"host" :"localhost"},
	xdb_path = "/var/xapian/blogs"
))
Author.build_table()
```

Usage
-----
```python
author = Author()
author.username = "admin"
author.nick = "Great Author"
author.intro = "I love blog"
author.save()

blog = Blog()
blog.author = author
blog.title = "Hello world"
blog.content = "foo bar"
blog.tags = ["life", "testing"]
blog.save()

for author in Author.walk_all_objs():
    print author.nick

authors = Author.search(key = "love", pos = 0, count = 10)

author = Author.find_one(username = "admin")
blogs = Blog.find_all_objs(author = author, tags = "life")
blogs = Blog.find_all_objs(author = author, tags = "testing")
```

Performance
============
To be added.