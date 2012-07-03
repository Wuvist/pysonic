# encoding: utf-8

import sys
import os
import redis as redispy
from cStringIO import StringIO
import config
import xapian

redis = None
_xdb = None
_xdb_readonly = None

def open_xdb():
	global _xdb

	# only close xdb when in debug
	if config.debug:
		if _xdb:
			_xdb.close()
		_xdb = xapian.WritableDatabase(config.xdb_path, xapian.DB_CREATE_OR_OPEN)
	return _xdb

def close_xdb():
	global _xdb

	if config.debug and _xdb:
		_xdb.close()
		_xdb = None

def setup():
	global redis, _xdb, _xdb_readonly

	redis = redispy.Redis(*config.default_rs)
	
	if config.xdb_path:
		if not config.debug:
			_xdb = xapian.WritableDatabase(config.xdb_path, xapian.DB_CREATE_OR_OPEN)
			_xdb_readonly = _xdb
		else:
			_xdb_readonly = xapian.Database(config.xdb_path)

saved_keys = dict()
def save_key(category, key):
	try:
		saved_key = saved_keys[category]
	except KeyError:
		saved_keys[category] = set()
		saved_key = set()

	if not isinstance(key, unicode):
		key = key.decode("utf-8")
	
	if key in saved_key:
		return
	
	saved_key.add(key)
	keys = "keys:%s" % category
	if redis.zscore(keys, key) != None:
		return

	for j in range(1, len(key) +1):
		redis.zadd(keys,  **{key[0:j].encode("utf-8"): 0})
	key = key.encode("utf-8")    
	redis.zadd(keys, **{key + "*": 0})

def search_key(category, word):
	keys_key = "keys:%s" % category    
	i = redis.zrank(keys_key, word)
		
	results = []
	if i > -1:
		keys = redis.zrange(keys_key, i, i + 100)
		for key in keys:
			if key.startswith(word):
				if key.endswith("*"):
					results.append(key[:-1].decode("utf-8"))
					if len(results) > 9:
						break
			else:
				break
	return results


def get_pos(body, tok, text):
	left = len(body[0:tok.start].decode("utf8"))
	right = len(text) + left
	return left, right
	
def delete_doc(uuid):
	xdb = open_xdb()
	xdb.delete_document(uuid)
	close_xdb()

def is_cn(i):
	return 0x4e00<=ord(i)<0x9fa6

def is_en(i):
	o = ord(i)
	return o<128

def bin_sep(text):
	if len(text) == 1:
		yield text
	else:
		for i in range(0, len(text) - 1):
			yield text[i:i+2]
	return

def sep_text(text):
	word = ""

	in_en = False
	in_cn = False

	for c in text:
		if is_cn(c):
			if in_en:
				if word:
					yield word
				word = ""
				in_en = False
			in_cn = True
			word += c
		elif c.isalnum():
			if in_cn:
				if word:
					for p in bin_sep(word):
						yield p
				word = ""
				in_cn = False
			in_en = True
			word += c
		else:
			if word:
				if in_cn:
					for p in bin_sep(word):
						yield p
					in_cn = False
				else:
					yield word
					in_en = False
				word = ""
			
	if word:
		if in_cn:
			for p in bin_sep(word):
				yield p
		else:
			yield word

	return

def save_doc(uuid, terms, fields, values = []):
	xdb = open_xdb()
	doc = xapian.Document()
	doc.add_term(uuid)
	doc.set_data(uuid)

	for v in values:
		doc.add_value(v)

	for term in terms:
		doc.add_term(term)
	
	for field_name, v in fields.iteritems():
		prefix = "X" + field_name.upper()
		text = v[0]
		if isinstance(text, str):
			text = text.decode("utf-8")

		text = text.lower()

		token_count = 0

		for word in sep_text(text):
			doc.add_posting(prefix + word, token_count, v[1])
			token_count += 1
		
		if len(text) < 50 and token_count > 1:
			doc.add_posting(prefix + text, 0, v[1])

	xdb.replace_document(uuid, doc)
	close_xdb()

def search_doc(query_string, start, count, prefixes = {}):
	xdb = _xdb_readonly
	qp = xapian.QueryParser()
	enquire = xapian.Enquire(xdb)

	for k, v in prefixes.iteritems():
		qp.add_prefix(k, v)

	qp.set_database(xdb)

	query = qp.parse_query(query_string)
	enquire.set_query(query)
	try:
		result = enquire.get_mset(start, count)
	except xapian.DatabaseModifiedError:
		xdb.reopen()
		result = enquire.get_mset(start, count)

	return result

def test():
	class obj(object):
		pass
	msg = {}
	msg[u"月夜逢燕侣 一红一绿两饺，红饺馅有胡萝卜，绿饺馅有香菜，喻指红男绿女，汤底运用猪骨高汤，汤面配以三朵形如飞燕的夜香花装饰，夜香花本身也带有特殊的香味。——南方都市报"] = 10

	save_doc("Test:1", [], msg)
	print search_doc([], "特殊", 0, 2)

if __name__ == '__main__':
	test()
