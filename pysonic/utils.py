# -*- coding: utf-8 -*-

import cPickle as pickle
import zlib

def get_id_or_value(value):
	if hasattr(value, "id"):
		return value.id
	else:
		return value

def get_type_id_or_value(value):
	type_name = value.__class__.__name__
	return type_name + str(get_id_or_value(value))

def pz_encode(obj):
	return zlib.compress(pickle.dumps(obj))

def pz_decode(data):
	return pickle.loads(zlib.decompress(data))