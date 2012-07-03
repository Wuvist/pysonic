# -*- coding: utf-8 -*-

from tornado import database
import redis
from field import *
from utils import *
import fulltext
import config
			
class MetaSonicObj(type):
	def __new__(meta, name, bases, attrs):
		cls = type.__new__(meta, name, bases, attrs)
		if cls.__name__ != "SonicObj":
			cls._setup()
		return cls

def SonicObjSetup(db, rs):
	def inner(cls):
		cls.setup(db, rs)
		return cls
	return inner

_dbs = {}

class SonicObj(object):
	__metaclass__ = MetaSonicObj

	@classmethod
	def _setup(cls):
		#todo: in future, should be able to setup different db / rs for different SonicObj
		try:
			cls._db = _dbs[str(config.default_db)]
		except KeyError:
			cls._db = database.Connection(*config.default_db)
			_dbs[str(config.default_db)] = cls._db
			
		cls._db_name = config.default_db[1]
		cls._rs = redis.Redis(**config.default_rs)
		cls._agent = None

		if not hasattr(cls, "_fields"):
			cls._fields = []
			cls._field_names = set()
			cls._field_types = {}
			cls.pre_commits = set()
			cls.after_commits = set()
			cls.after_deletes = set()

		for k, v in cls.__dict__.iteritems():
			field = v

			if isinstance(field, SonicField) and k not in cls._field_names:
				field.name = k
				cls._fields.append(field)
				cls._field_names.add(field.name)
				cls._field_types[field.name] = field

		cls._fields.sort(key=lambda a: a.creation_counter)

		cls._indexes = []
		cls._uniques = []
		cls._fulltext_indexes = []


		if hasattr(cls, "indexes"):
			#todo: must check index field, alert case like put two list field in same index
			i = 0
			for k, v in cls.indexes.iteritems():
				order_by = cls.get_order_by(i)
				item = []
				for f in v:
					item.append(f.name)
				cls._indexes.append(index(item, order_by))
				i += 1

		if hasattr(cls, "uniques"):
			for k, v in cls.uniques.iteritems():
				item = []
				for f in v:
					 item.append(f.name)
				cls._indexes.append(index(item))
				cls._uniques.append(item)

		#todo: should check if fulltext_indexes definition valid here
		if not hasattr(cls, "fulltext_indexes"):
			cls.fulltext_indexes = None

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
		if type(value) == str:
			value = value.decode("utf-8")

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
		self.id = row["id"]
		self.is_deleted = row["is_deleted"]
		binary_data = row["data"]

		self.from_binary(binary_data, lazy_load)

	def from_binary (self, binary_data, lazy_load = True):
		data = pz_decode(binary_data)
		try:
			self.id = data["id"]
		except KeyError:
			pass

		for field in self._fields:
			key = field.name

			try:
				value = data[key]
				value = field.from_value(value, self, lazy_load)
			except KeyError:
				value = field.get_default()

			object.__setattr__(self, key, value)

	def to_binary(self):
		data = {}
		data["id"] = self.id
		for field in self._fields:
			key = field.name
			obj = self.__getattribute__(key)
			if obj != None:
				data[key] = field.to_value(obj)
					
		return pz_encode(data)

	def check_unique (self):
		for index in self._uniques:
			count = 0
			data = {}
			need_checking = self.is_new
			for field_name in index:
				data[field_name] = self.__dict__[field_name]
				if (not need_checking) and (field_name in self.dirty_fields):
					need_checking = True

			if need_checking:
				count = self.count(**data)
			
			if count > 0:
				raise Exception("duplicate value: " + str(data))

	def _send_update_fulltext_msg(self):
		if self.fulltext_indexes == None: return

		if config.agent:
			config.agent.send("fulltext", "update", 
				(self.__class__.__name__, self.id))
		else:
			self.update_fulltext_indexes()

	def _send_delete_fulltext_msg(self):
		if self.fulltext_indexes == None: return

		if config.agent:
			config.agent.send("fulltext", "delete", 
				self.__class__.__name__ + ":" + str(self.id))
		else:
			fulltext.delete_doc(self.__class__.__name__ + ":" + str(self.id))

	def update_fulltext_indexes(self):
		if self.fulltext_indexes == None: return
		
		terms = []
		data = {}
		terms.append("XCLS" + self.table_name.lower())
		if hasattr(self, "user"):
			if self.user != None:
				terms.append("XUSER%d" % self.user.id)
			  
		if hasattr(self, "tags"):
			if self.tags != None:
				for tag in self.tags:
					tag = tag.lower()
					terms.append("XTAG%s" % tag)

		for k, v in self.fulltext_indexes.iteritems():
			if k == 'groups':
				for field in v:
					field_value = self.__dict__[field.name]
					if field_value == None:
						continue

					if hasattr(field_value, "id"):
						terms.append("X" + field.name.upper() + str(field_value.id))
					else:
						if isinstance(field_value, bool):
							if field_value:
								#only interested with data has flag on
								terms.append("X" + field.name.upper() + "1")
						else:
							terms.append("X" + field.name.upper() + field_value)
			else:
				value = self.__dict__[k.name]
				if value:
					data[k.name] = (value, v)
		
		fulltext.save_doc("%s:%d" % (self.table_name, self.id), terms , data)


	def is_fulltext_indexes_dirty(self):
		if self.fulltext_indexes == None:
			return False

		for k, v in self.fulltext_indexes.iteritems():
			if isinstance(k, str):
				continue
			if k.name in self.dirty_fields:
			   return True

		if "tags" in self.dirty_fields:
			return True

		if "user" in self.dirty_fields:
			return True

		return False

	def _insert(self):
		self.check_unique()
		sql = "insert into `%s` (data)values(%%s)" % self.table_name
		self.id = self._db.execute(sql, self.to_binary())
		self._rs.zadd(self.table_name + "_ids", **{str(self.id):self.id})

		for index in self._indexes:
			keys = self.get_index_keys(index.field_names)
			for key in keys:
				if index.order_by:
					order_field = self.__dict__[index.order_by]
					if order_field == None:
						score = 0
					else:
						score = int(order_field)
					self._rs.zadd(key, **{str(self.id): score})
				else:
					self._rs.zadd(key, **{str(self.id): self.id})

	def _update(self):
		if len(self.dirty_fields) == 0:
			return False

		# todo: must check unique here
		sql = "update `%s` set data=%%s where id=%%s" % self.table_name
		self._db.execute(sql, self.to_binary(), self.id)

		for index in self._indexes:
			need_update = False
			if index.order_by in self.dirty_fields:
				need_update = True

			old = {}
			for field_name in index.field_names:
				if field_name in self.dirty_fields:
					need_update = True
					old[field_name] = self.dirty_values[field_name]
				else:
					old[field_name] = self.__dict__[field_name]

			if need_update:
				if len(old) > 0:
					keys = self.get_keys(**old)
					for key in keys:
						self._rs.zrem(key, self.id)

				keys = self.get_index_keys(index.field_names)
				for key in keys:
					if index.order_by:
						order_field = self.__dict__[index.order_by]
						if order_field == None:
							score = 0
						else:
							score = int(order_field)
						self._rs.zadd(key, **{str(self.id): score})
					else:
						self._rs.zadd(key, **{str(self.id): self.id})

	def save(self):
		for pre_commit in self.pre_commits:
			pre_commit(self)
			
		if self.is_new:
			self._insert()
			self._send_update_fulltext_msg()
		else:
			if len(self.dirty_fields) == 0:
				return False
			self._update()

			if self.is_fulltext_indexes_dirty():
				self._send_update_fulltext_msg()
			
		for after_commit in self.after_commits:
			after_commit(self)
			
		self.is_new = False
		self.dirty_fields = set()
		self.dirty_values = {}

		return True

	def clear_index (self):
		for index in self._indexes:
			keys = self.get_index_keys(index.field_names)
			for key in keys:
				self._rs.zrem(key, self.id)

		self._rs.zrem(self.table_name + "_ids", self.id)
		self._send_delete_fulltext_msg()

	@classmethod
	def _get_field_type(cls, key):
		return cls._field_types[key]

	@classmethod
	def delete_obj(cls, obj):
		obj.clear_index()
		sql = "update `%s` set is_deleted=1 where id=%%s" % (cls.__name__)
		cls._db.execute(sql, obj.id)        
		
		for after_delete in cls.after_deletes:
			after_delete(obj)

	@classmethod
	def delete_objs(cls, objs):
		for obj in objs:
			if obj:                
				obj.clear_index()
		ids = [str(obj.id) for obj in objs]
		sql = "update `%s` set is_deleted=1 where id in (%s)" % (cls.__name__, ",".join(ids))
		cls._db.execute(sql)

		for obj in objs:
			for after_delete in cls.after_deletes:
				after_delete(obj)

	@classmethod
	def delete_ids(cls, ids):
		objs = cls.get(ids)
		cls.delete_objs(objs)

	@classmethod
	def delete_id(cls, id):
		obj = cls.get(id)
		cls.delete_obj(obj)
		
	@classmethod
	def delete(cls, data):
		if type(data) in (int, long):
			return cls.delete_id(data)
		elif isinstance(data, SonicObj):
			return cls.delete_obj(data)
		elif isinstance(data, list):
			if len(data) == 0:
				return
			if type(data[0]) in (int, long):
				return cls.delete_ids(data)
			return cls.delete_objs(data)
		raise Exception("Invalid parameter data" + str(type(data)))
			
	@classmethod
	def destroy(cls, id):
		if type(id) == list:
			objs = cls.get(id)
			for obj in objs:
				if obj:                
					obj.clear_index()
			sql = "delete from `%s` where id in (%s)" % (cls.__name__, ",".join(map(str, id)))
			cls._db.execute(sql)
		else:
			obj = cls.get(id)
			if obj:
				obj.clear_index()
			sql = "delete from `%s` where id=%%s" % (cls.__name__)
			cls._db.execute(sql, id)

	@classmethod
	def get_create_table_sql(cls):
		sql = """
		CREATE TABLE IF NOT EXISTS `%s`(
		id int AUTO_INCREMENT NOT NULL,
		data mediumblob,
		is_deleted boolean DEFAULT 0,
			PRIMARY KEY (`id`))
		ENGINE = InnoDB;
		""" % (cls.__name__)
		return sql

	@classmethod
	def build_table(cls):
		if not cls._db.get("SHOW TABLES LIKE '" + cls.__name__ + "'"):
			cls._db.execute(cls.get_create_table_sql())

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
			try:
				obj = cls(row, lazy_load)
			except AttributeError:
				return None
			obj.id = int(id)
			return obj

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
			try:
				result[pos] = cls(row, lazy_load)
			except AttributeError:
				continue
			result[pos].id = int(row["id"])

		return result

	'''
	Get one random id
	'''
	@classmethod
	def get_random_id(cls):
		key = cls.__name__ + "_ids"
		count = cls._rs.zcard(key)
		import random
		pos = random.randint(0, count - 1)
		ids = cls._rs.zrange(key, pos, pos + 1)
		return int(ids[0])

	'''
	Get one random obj
	'''
	@classmethod
	def get_random_obj(cls):
		return cls.get(cls.get_random_id())

	'''
	Always return id in descending order
	if curor is not found, will return from the begining, i.e. largest ids
	'''
	@classmethod
	def get_ids(cls, count, cursor = -1, forwarding = True):
		pos = -1
		if cursor > -1:
			if forwarding:
				pos = cls._rs.zrevrank(cls.__name__ + "_ids", cursor)
			else:
				pos = cls._rs.zrank(cls.__name__ + "_ids", cursor)
			if pos == None:
				pos = -1
				forwarding = True

		if forwarding:
			ids = cls._rs.zrevrange(cls.__name__ + "_ids", pos + 1, pos + count)
		else:
			ids = cls._rs.zrange(cls.__name__ + "_ids", pos + 1, pos + count)
			ids.reverse()
		ids = map(int, ids)
		return ids

	@classmethod
	def get_objs(cls, count, cursor = -1, forwarding = True):
		return cls.get(cls.get_ids(count, cursor, forwarding))
	
	def get_index_keys(self, field_names):
		kwarg = {}

		for field_name in field_names:
			kwarg[field_name] = self.__dict__[field_name]

		return self.get_keys(**kwarg)

	@classmethod
	def get_keys(cls, **kwarg):
		key = cls.__name__
		keys = kwarg.keys()
		keys.sort()
		has_list_field = False
		
		# todo: kind of messy here, should refactor

		for k in keys:
			value = kwarg[k]

			if value == None:
				key += ":" + k  + ":"
			else:
				if isinstance(value, list):
					if len(value) == 0:
						key += ":" + k  + ":"
					else:
						has_list_field = True
						list_field = value
						list_field_key = k
						key += ":" + k  + ":%s"
				else:
					if cls._get_field_type(k).type == "dynamic":
						cls_name = value.__class__.__name__
						if cls_name == "LazyField":
							cls_name = value._cls.__name__
						key += ":" + k  + ":%s%s" % (cls_name, get_id_or_value(value))
					else:
						if cls._get_field_type(k).type == "dynamic_list":
							key += ":" + k  + ":%s" % get_type_id_or_value(value)
						else:
							key += ":" + k  + ":%s" % get_id_or_value(value)

		if has_list_field:
			if cls._get_field_type(k).type == "dynamic_list":
				keys =[key.replace(":" + list_field_key + ":%s", ":" + 
					list_field_key + ":" + unicode(get_type_id_or_value(v))) for v in list_field]
			else:
				keys =[key.replace(":" + list_field_key + ":%s", ":" + 
					list_field_key + ":" + unicode(get_id_or_value(v))) for v in list_field]
		else:
			keys = [key]
	
		return keys

	@classmethod
	def find_all(cls, desc = True, **kwarg):
		key = cls.get_keys(**kwarg)[0]
		if desc:
			ids  = cls._rs.zrevrange(key, 0, -1)
		else:
			ids  = cls._rs.zrange(key, 0, -1)
		return map(int, ids)

	@classmethod
	def find_all_ids(cls, desc = True, **kwarg):
		return cls.find_all(desc, **kwarg)

	@classmethod
	def find_all_objs(cls, desc = True, **kwarg):
		ids = cls.find_all(desc, **kwarg)
		return cls.get(ids)

	@classmethod
	def _find_with_score(cls, count, desc, order_keys, **kwarg):
		min_score = "-inf"
		max_score = "+inf"

		for k, v in order_keys.iteritems():
			if k.endswith("__lte"):
				max_score = v
			elif k.endswith("__lt"):
				max_score = "(" + str(v)
			elif k.endswith("__gte"):
				min_score = v
			elif k.endswith("__gt"):
				min_score = "(" + str(v)        

		key = cls.get_keys(**kwarg)[0]

		if desc:
			ids  = cls._rs.zrevrangebyscore(key, max_score, min_score, 0, count)
		else:
			ids  = cls._rs.zrangebyscore(key, min_score, max_score, 0, count)

		return map(int, ids)
	
	@classmethod
	def find(cls, count, cursor = -1, desc = True, **kwarg):
		order_keys = {}
		for k in kwarg.keys():
			if "__" in k:
				order_keys[k] = kwarg[k]

		for k in order_keys.keys():
			del kwarg[k]

		if len(order_keys) > 0:
			return cls._find_with_score(count, desc, order_keys, **kwarg)

		key = cls.get_keys(**kwarg)[0]
		if cursor == -1:
			start = 0
		else:
			if desc:
				start = cls._rs.zrevrank(key, cursor)
			else:
				start = cls._rs.zrank(key, cursor)
			if start == None:
				start = 0
		stop = start + count - 1

		if desc:
			ids  = cls._rs.zrevrange(key, start, stop)
		else:
			ids  = cls._rs.zrange(key, start, stop)

		#todo: must be better way to check index
		if len(ids) == 0:
			invalid_keys = [key for key in kwarg.keys() if key not in cls._field_names]
			if len(invalid_keys) > 0:
				raise Exception("Invalid keys: " + str(invalid_keys))
		return map(int, ids)

	@classmethod
	def find_objs(cls, count, cursor = -1, desc = True, **kwarg):
		ids = cls.find(count, cursor, desc, **kwarg)
		return cls.get(ids)

	@classmethod
	def find_one_id(cls, **kwarg):
		key = cls.get_keys(**kwarg)[0]
		ids = map(int, cls._rs.zrange(key, 0, 1))
		if len(ids) > 0:
			return ids[0]
		return None

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

	@classmethod
	def count(cls, **kwarg):
		key = cls.get_keys(**kwarg)[0]
		return cls._rs.zcard(key)

	@classmethod
	def get_order_by(cls, i):
		key = cls.indexes.keys()[i]
		try:
			pos = key.index("order_by_")
			return key[pos + 9:]
		except:
			pass
		return None

	@classmethod
	def rebuild_all (cls):
		keys = cls._rs.keys(cls.__name__ + ":*")
		for key in keys:
			cls._rs.delete(key)
		cls.rebuild_ids()
		cls.rebuild_indexes()
		cls.rebuild_fulltext_indexes()

	@classmethod
	def rebuild_ids (cls):
		sql = "select id from %s where is_deleted = 0" % cls.__name__
		key = cls.__name__ + "_ids"
		rows = cls._db.query(sql)
		cls._rs.delete(key)
		for row in rows:
			id = row["id"]
			cls._rs.zadd(key, **{str(id): id})

	@classmethod
	def rebuild_indexes (cls):
		for index in cls._indexes:
			cls.rebuild_index(index.field_names, index.order_by)

	@classmethod
	def rebuild_fulltext_indexes (cls):
		#todo: should be able to rebuild single fulltext index by passing parameter
		for obj in cls.walk_all_objs():
			obj._send_update_fulltext_msg()

	@classmethod
	def rebuild_index (cls, arg, order_by = None):
		"""
		Rebuild one index, arg is a list of field names
		"""
		for obj in cls.walk_all_objs():        
			keys = obj.get_index_keys(arg)
			for key in keys:
				if order_by == None:
					cls._rs.zadd(key, **{str(obj.id): obj.id})
				else:
					order_field = obj.__dict__[order_by]
					if order_field == None:
						score = 0
					else:
						score = int(order_field)
					cls._rs.zadd(key, **{str(obj.id): score})

	@classmethod
	def walk_all_objs (cls, count = 100, cursor = -1):
		while True:
			ids = cls.get_ids(count, cursor)
			if len(ids) == 0:
				return 
			for obj_id in ids:
				obj = cls.get(obj_id)
				if obj:
					yield obj
				else:
					cls._rs.zrem(cls.__name__ + "_ids", id)

				cursor = obj_id

	@classmethod
	def get_fulltext_fields(cls):
		fields = []
		if cls.fulltext_indexes:
			for k, v in cls.fulltext_indexes.iteritems():
				if not isinstance(k, str):
					fields.append(k.name)

		if hasattr(cls, "tags"):
			fields.append("tag")

		return fields

	@classmethod
	def search(cls, key, pos, count, terms = {}):
		return cls.search_by_field(cls.get_fulltext_fields(), key, pos, count, terms)

	@classmethod
	def get_search_query_and_prefix(cls, fields, key, terms = {}):
		query_string = "class:" + cls.__name__.lower() + " "

		prefixes = {}
		if len(fields) > 0:
			query_string += "AND ("
			for field in fields:
				prefix = field.lower()
				prefixes[prefix] = "X" + field.upper()

				query_string += prefix + ":" + key + " "
			query_string += ")"

		for field, value in terms.iteritems():
			prefix = field.lower()
			prefixes[prefix] = "X" + prefix.upper()
			if hasattr(value, "id"):
				query_string += " AND %s:%s " % (prefix, str(value.id)) 
			else:
				query_string += " AND %s:%s " % (prefix, value)

		prefixes['class'] = 'XCLS'

		return query_string, prefixes

	@classmethod
	def search_by_field(cls, fields, key, pos, count, terms = {}):
		query_string, prefixes= cls.get_search_query_and_prefix(fields, key, terms = {})
		
		result = fulltext.search_doc(query_string, pos, count, prefixes)

		ids = []
		for r in result:
			data = r.document.get_data()
			item_type, item_id = data.split(":")
			ids.append(int(item_id))

		return ids


	def __eq__(self, other):
		if other == None:
			return False
		return self.id == other.id

	def __ne__(self, other):
		if other == None:
			return True
		return self.id != other.id

	def __int__(self):
		return self.id

	def __str__(self):
		return self.__class__.__name__ + str(self.id)