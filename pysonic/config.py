# encoding: utf-8

debug = True
default_db = ("localhost", "test", "root")
default_rs = {"host" :"localhost"}
xdb_path = None
agent = None

def setup(settings):
	g = globals()
	for k in g:
		try:
			g[k] = settings[k]
		except KeyError:
			pass    

	import fulltext, obj
	fulltext.setup()
	obj.setup()
