from tornado import database
import inspect
from field import *
from utils import *
import re
import config
			
class MetaSqlSonicObj(type):
	def __new__(meta, name, bases, attrs):
		cls = type.__new__(meta, name, bases, attrs)
		if cls.__name__ != "SqlSonicObj":
			cls.setup()
		return cls

def SqlSonicObjSetup(db):
	def inner(cls):
		cls.setup(db)
		return cls
	return inner

class SqlSonicObj(object):
	__metaclass__ = MetaSqlSonicObj

	@classmethod
	def setup(cls, db = None):
		db = db if db else config.default_db
		
		cls._db = database.Connection(*db)
		cls._db_name = db[1]

		if not hasattr(cls, "_fields"):
			cls._fields = []
			cls._field_names = set()
			cls.after_commits = set()
			cls.after_deletes = set()

		for k, v in cls.__dict__.iteritems():
			field = v

			if isinstance(field, SonicField) and k not in cls._field_names:
				field.name = k
				cls._fields.append(field)
				cls._field_names.add(field.name)

	def __init__ (self, row = None, lazy_load = True):
		object.__setattr__(self, "is_new", (row == None))
		self.id = None
		self.table_name = self.__class__.__name__
		self.dirty_fields = set()
		self.dirty_values = {}

		if row:
			self.from_db_row(row, lazy_load)
		else:            
			for field in self._fields:
				key = field.name
				if isinstance(field.type, str):
					self.__setattr__(key, field.get_default())
				else:
					self.__setattr__(key, None)
					
	def keep_old_value(self, field_name):
		self.dirty_fields.add(field_name)
		if not self.dirty_values.has_key(field_name):
			try:
				self.dirty_values[field_name] = self.__dict__[field_name]
			except KeyError:
				# todo: that mean the old value is null, should take care this situation?
				# will indexing null value useful?
				pass

	def __setattr__ (self, name, value):
		if self.is_new:
			 object.__setattr__(self, name, value)
			 return None
		
		if name in self._field_names:
			old_value = self.__dict__[name]
			
			if isinstance(old_value, LazyField):
				if value == None or value.id != old_value.id:
					self.keep_old_value(name)
			elif value != old_value:
				self.keep_old_value(name)

		object.__setattr__(self, name, value)
	   

	def from_db_row (self, row, lazy_load = True):
		data = pz_decode(row["data"])
		self.id = row["id"]
		self.is_deleted = row["is_deleted"]

		for field in self._fields:
			key = field.name

			try:
				value = data[key]
				object.__setattr__(self, key, field.from_value(value, self, lazy_load))
			except KeyError:
				object.__setattr__(self, key, field.get_default())

	def save(self):
		if self.is_new:
			cols = []
			tmp = []
			data = []
			for field in self._fields:
				cols.append(field.name)
				tmp.append("%s")
				data.append(self.__dict__[field.name])
			sql = "insert into `%s` (%s)values(%s)" % (self.table_name, ",".join(cols), ",".join(tmp))
			self.id = self._db.execute(sql, *data)
		else:
			if len(self.dirty_fields) == 0:
				return False

			# todo: must check unique here
			sql = "update `%s` set data=%%s where id=%%s" % self.table_name
			self._db.execute(sql, self.to_binary(), self.id)

		for after_commit in self.after_commits:
			after_commit(self)
			
		self.is_new = False
		self.dirty_fields = set()
		self.dirty_values = {}

		return True

	@classmethod
	def delete(cls, id):
		if type(id) == list:
			sql = "update `%s` set is_deleted=1 where id in (%s)" % (cls.__name__, ",".join(map(str, id)))
			cls._db.execute(sql)
		else:
			sql = "update `%s` set is_deleted=1 where id=%%s" % (cls.__name__)
			cls._db.execute(sql, id)

	@classmethod
	def destroy(cls, id):
		if type(id) == list:
			sql = "delete from `%s` where id in (%s)" % (cls.__name__, ",".join(map(str, id)))
			cls._db.execute(sql)
		else:
			sql = "delete from `%s` where id=%%s" % (cls.__name__)
			cls._db.execute(sql, id)

	@classmethod
	def get_create_table_sql(cls):
		cols = []
		for field in cls._fields:
			if field.type == "str":
				cols.append("%s VARCHAR(2000) CHARACTER SET utf8 COLLATE utf8_general_ci " % field.name)
			elif field.type == "int":
				cols.append("%s INT(10)" % field.name)
			elif field.type == "float":
				cols.append("%s FLOAT(10, 10)" % field.name)	
		sql = """
		CREATE TABLE `%s`(
		id int AUTO_INCREMENT NOT NULL,
		%s,
		is_deleted boolean DEFAULT 0,
			PRIMARY KEY (`id`))
		ENGINE = InnoDB;
		CHARACTER SET utf8;
		""" % (cls.__name__, ",".join(cols))

		return sql

	@classmethod
	def build_table(cls, print_error = True):
		try:
			cls._db.execute(cls.get_create_table_sql())
			print "built: " + cls.__name__
		except:
			if print_error:
				print "built failed: " + cls.__name__

	@classmethod
	def get(cls, id, lazy_load = True, read_deleted = False):
		if type(id) == list:
			# todo: this lazy_load will iterate through all obj's obj field
			# better, use fetchByIds and assign them together
			return cls.fetchByIds(id, lazy_load, read_deleted)
		return cls.fetchById(id, lazy_load, read_deleted)

	@classmethod
	def fetchById(cls, id, lazy_load = True, read_deleted = False):
		sql = "select * from %s where id=%%s" % (cls.__name__)
		for row in cls._db.query(sql, id):
			if read_deleted == False and row["is_deleted"] == True:
				return None
			return cls(row, lazy_load)

		return None

	@classmethod
	def fetchByIds(cls, ids, lazy_load = True, read_deleted = False):
		if len(ids) ==0:
			return []
		sql = "select * from %s where id in (%s)" % (cls.__name__, ",".join(map(str, ids)))
		result = [None for i in range(len(ids))]
		for row in cls._db.query(sql):
			if read_deleted == False and row["is_deleted"] == True:
				continue
			pos = ids.index(row["id"])
			result[pos] = cls(row, lazy_load)

		return result

	'''
	Always return id in descending order
	if curor is not found, will return from the begining, i.e. largest ids
	'''
	@classmethod
	def get_ids(cls, count, cursor = -1, forwarding = True):
		pass

	
	@classmethod
	def find(cls, count, cursor = -1, **kwarg):
		pass

	@classmethod
	def find_objs(cls, count, cursor = -1, **kwarg):
		ids = cls.find(count, cursor, **kwarg)
		return cls.get(ids)

	@classmethod
	def find_one_id(cls, **kwarg):
		pass

	@classmethod
	def find_one_obj(cls, **kwarg):
		return cls.find_one(**kwarg)

	@classmethod
	def find_one(cls, **kwarg):
		obj_id = cls.find_one_id(**kwarg)
		if obj_id == None:
			return None
		return cls.get(obj_id)

	@classmethod
	def find_one_or_new(cls, **kwarg):
		obj = cls.find_one(**kwarg)
		if obj == None:
			obj = cls()
			for key in kwarg.keys():
				obj.__dict__[key] = kwarg[key]

		return obj

		
	def __eq__(self, other):
		if other == None:
			return False
		return self.id == other.id

	def __ne__(self, other):
		if other == None:
			return True
		return self.id != other.id
