from utils import *
from datetime import datetime
Now = "Now"

class LazyField(object):
	def __init__(self, obj, key, cls, id):
		self._obj = obj
		self._key = key
		self._cls = cls
		self.id = id
		self._loaded = False

	def _ensure_loaded(self):
		if self._loaded:
			return
		
		self._real_obj = self._cls.get(self.id)
		# self._obj.__dict__[self._key] = self._real_obj
		self._loaded = True

	def __getattr__(self, key):
		self._ensure_loaded()
		return self._real_obj.__getattribute__(key)

	def __unicode__(self):
		self._ensure_loaded()
		return unicode(self._real_obj)

	def __eq__(self, other):
		if other == None:
			return False
		return self.id == other.id

	def __ne__(self, other):
		if other == None:
			return True
		return self.id != other.id

	def __nonzero__(self):
		self._ensure_loaded()
		return bool(self._real_obj)

	def __int__(self):
		return self.id

class SonicField(object):
	creation_counter = 0
	
	def __init__(self, field_name, field_type, *field_default):
		self.name = field_name
		self.type = field_type
		self.default = None

		self.is_obj = not isinstance(field_type, str)
		
		if len(field_default) > 0:
			self.default = field_default[0]
			
		#use creation counter to ensure field order same as source code
		self.creation_counter = SonicField.creation_counter
		SonicField.creation_counter += 1

	def get_default(self):
		if callable(self.default):
			return self.default()
		return self.default

	def to_value(self, obj):
		def dynamic_to_val(obj):
			if obj == None:
				return [None, None]
			cls = type(obj)
			if cls in (int, str, float):
				return [cls, obj]
			
			if cls.__name__ == "LazyField":
				cls = obj._cls
			return [cls, obj.id]

		def emobj_to_val(obj):
			if obj == None:
				return None
			return obj.to_binary()

		if self.type == "dynamic":
			return dynamic_to_val(obj)
		elif self.type == "dynamic_list":
			return [dynamic_to_val(item) for item in obj]
		elif self.type == "emobj":
			return emobj_to_val(obj)
		elif self.type == "emobj_list":
			return [emobj_to_val(item) for item in obj]

		if isinstance(self.type, str):
			if self.type == "obj_list":
				return [o.id for o in obj]
			return obj
		return obj.id

	def to_json_value(self, obj):
		def dynamic_to_val(obj):
			if obj == None:
				return [None, None]
			cls = type(obj)
			if cls in (int, str, float):
				return obj
			
			return obj.id
		
		if self.type == "dynamic":
			return dynamic_to_val(obj)
		elif self.type == "dynamic_list":
			return [dynamic_to_val(item) for item in obj]
		elif self.type == "date":
			#todo, should be using ISO format etc
			return format_date(obj)

		if isinstance(self.type, str):
			if self.type == "obj_list":
				return [o.id for o in obj]
			return obj
		return obj.id

	def from_value(self, val, obj, lazy_load):
		if val == None:
			return None

		def val_to_dynamic(val):
			(obj_type, obj_id) = (val[0], val[1])
			
			if obj_type == None: return None
			if obj_type in (int, str, float):
				return val[1]
			
			if lazy_load:
				return LazyField(obj, self.name, obj_type, obj_id)
			else:
				return obj_type.get(obj_id, lazy_load = lazy_load)

		def val_to_emobj(val):			
			if val == None: return None
			obj = self.member_type()
			obj.from_binary(val)
			return obj

		if self.type == "emobj":
			return val_to_emobj(val)

		if self.type == "emobj_list":
			return [val_to_emobj(item) for item in val]

		if self.type == "dynamic":
			return val_to_dynamic(val)

		if self.type == "dynamic_list":
			return [val_to_dynamic(item) for item in val]

		if self.is_obj:            
			if lazy_load:
				return LazyField(obj, self.name, self.type, val)
			else:
				return self.type.get(val, lazy_load = lazy_load)
		else:
			if self.type == "obj_list":
				if lazy_load:
					return [LazyField(obj, self.name, self.member_type, obj_id) for obj_id in val]
				else:
					return self.member_type.get(val, lazy_load = lazy_load)
			
			return val


def int_field (*argv):
	name = None
	return SonicField(name, "int", *argv)

def float_field (*argv):
	name = None
	return SonicField(name, "float", *argv)

def str_field (*argv):
	name = None
	return SonicField(name, "str", *argv)

def text_field (*argv):
	name = None
	return SonicField(name, "text", *argv)

def date_field (*argv):
	name = None
	if argv[0] == Now:
		argv = [datetime.now]
	return SonicField(name, "date", *argv)
	
def bool_field (*argv):
	name = None
	return SonicField(name, "bool", *argv)

def var_field (*argv):
	name = None
	return SonicField(name, "var", *argv)

def obj_field (member_type):
	name = None

	field = SonicField(name, member_type)
	return field

def emobj_field (member_type):
	name = None

	field = SonicField(name, "emobj")
	field.member_type = member_type
	return field

def emobj_list_field (member_type):
	name = None

	field = SonicField(name, "emobj_list", [])
	field.member_type = member_type
	return field

def list_field (member_type):
	name = None

	if type(member_type) == type:
		field = SonicField(name, "list", [])
	else:
		field = SonicField(name, "obj_list", []) 
	field.member_type = member_type
	return field

def dynamic_field ():
	name = None
	field = SonicField(name, "dynamic") 
	field.member_type = "dynamic"
	return field

def dynamic_list_field ():
	name = None
	field = SonicField(name, "dynamic_list", [])
	field.member_type = "dynamic_list"
	return field

class index:
	def __init__(self, field_names, order_by = None):
		self.field_names = field_names
		self.order_by = order_by
