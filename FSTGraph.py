import sys
from runstats import Statistics
import string
import copy
import hashlib
import time


class Lexer:

	def __init__(self, features=[]):
		self.features = features

	def parse_query(self, query):
		months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
		geohash_buff = ''
		m = {}
		feature = ''
		for seg in query.split('.'):
			if seg[0].isdigit():
				# possible dates
				if seg[-1].isdigit():
					# year
					m['year'] = int(seg)
				else:
					# day or hour
					if seg.endswith('am') or seg.endswith('pm'):
						m['hour'] = int(seg[:-2]) + (12 if seg.endswith('pm') else 0)
					else:
						m['day'] = int(seg[:-2])
			else:
				#possible feature / month / @
				if seg[0] == '@':
					geohash_buff += seg[1:]
				elif seg in self.features:
					feature = seg
				else:
					m['month'] = months.index(seg) + 1

		return STC(SC(geohash_buff), TC(m)), feature

	def dtghv2insertion(self, dt, gh, val):
		m = {}
		m['year'] = dt.year
		m['month'] = dt.month
		m['day'] = dt.day
		m['hour'] = dt.hour
		return Insertion(STC(SC(gh), TC(m)), val)




class SC:
	def __init__(self, spatial_path_str=''):
		self.path = spatial_path_str
		self.depth = len(spatial_path_str)

	def __eq__(self, other):
		return self.path == other.path

	def __ne__(self, other):
		return not self == other

	def __lt__(self, other):
		this = self.path
		that = other.path
		return that.startswith(this) and self != other

	def __le__(self, other):
		this = self.path
		that = other.path
		return that.startswith(this)

	def __gt__(self, other):
		this = self.path
		that = other.path
		return this.startswith(that) and self != other

	def __ge__(self, other):
		this = self.path
		that = other.path
		return this.startswith(that)

	def __len__(self):
		return self.depth

	def __sub__(self, other):
		assert self > other
		return self.path[other.depth]

	def __str__(self):
		return self.path


class TC:
	levels = ['year', 'month', 'day', 'hour']

	def __init__(self, temporal_dict={}):
		for i in range(len(temporal_dict)):
			setattr(self, TC.levels[i], temporal_dict[TC.levels[i]])
		self.map = temporal_dict
		self.depth = len(temporal_dict)

	def __eq(self, other, depth):
		for i in range(depth):
			level = TC.levels[i]
			if getattr(self, level) != getattr(other, level):
				return False
		return True

	def __len__(self):
		return self.depth

	def __eq__(self, other):
		return len(self) == len(other) and self.__eq(other, len(self))

	def __ne__(self, other):
		return not self == other

	def __lt__(self, other):
		return len(other) > len(self) and other.__eq(self, len(self))

	def __le__(self, other):
		return self == other or self < other

	def __gt__(self, other):
		return len(other) < len(self) and self.__eq(other, len(other))

	def __ge__(self, other):
		return self == other or self > other

	def __sub__(self, other):
		assert self > other
		return TC.levels[other.depth], getattr(self, TC.levels[other.depth])

	def __str__(self):
		s = ''
		for i in range(self.depth):
			s += str(getattr(self, TC.levels[i])) + '/'
		return s[:-1]

class STC:
	def __init__(self, sc, tc):
		self.sc = sc
		self.tc = tc

	def __eq__(self, other):
		return hash(self) == hash(other)

	def __str__(self):
		if len(self.tc) == 0:
			return str(self.sc)
		elif len(self.sc) == 0:
			return str(self.tc)
		else:
			return str(self.tc) + '.' + str(self.sc)

	def __hash__(self):
		return hash(str(self))

	def __repr__(self):
		return str(self)

class Insertion:
	def __init__(self, stc, value):
		self.stc = stc
		self.value = value
		self.hash = hashlib.md5((str(self.stc) + str(int(round(time.time() * 1000)))).encode()).hexdigest()

class Node:
	ghchars = [str(i) for i in range(0, 10)] + [c for c in string.ascii_lowercase if c not in ['a', 'i', 'l', 'o']]

	def __init__(self, stc):
		self.stc = stc
		self.stats = Statistics()
		self.s = {}
		self.t = {}
		self.last_insertion = None

	def retrieve(self, stc, db):
		if stc == self.stc:
			return self.stats
		if stc.tc > self.stc.tc:
			diff_level, diff_value = stc.tc - self.stc.tc
			if diff_value in self.t.keys():
				return db[self.t[diff_value]].retrieve(stc, db)
			else:
				return None
		else:
			diff_char = stc.sc - self.stc.sc
			if diff_char in self.s.keys():
				return db[self.s[diff_char]].retrieve(stc, db)
			else:
				return None

	def insert(self, insertion, db):
		# print(f'insert({str(self.stc.tc), str(self.stc.sc)})')
		if insertion.hash == self.last_insertion:
			return
		self.stats.push(insertion.value)
		self.last_insertion = insertion.hash
		if insertion.stc == self.stc:
			# print("base case, insertion done")
			return

		if insertion.stc.tc > self.stc.tc:
			# insert temporal direction
			diff_level, diff_value = insertion.stc.tc - self.stc.tc
			# if diff_value in self.t.keys():
			# 	# has path, traverse down
			# 	return

			m = self.stc.tc.map.copy()
			m[diff_level] = diff_value
			new_stc = STC(SC(self.stc.sc.path), TC(m))
			if new_stc not in db:
				self.t[diff_value] = new_stc
				db[new_stc] = Node(new_stc)

			db[new_stc].insert(insertion, db)

		if insertion.stc.sc > self.stc.sc:
			# insert temporal direction
			diff_char = insertion.stc.sc - self.stc.sc

			new_stc = STC(SC(self.stc.sc.path + diff_char), TC(self.stc.tc.map))
			if new_stc not in db:
				self.s[diff_char] = new_stc
				db[new_stc] = Node(new_stc)
			db[new_stc].insert(insertion, db)

		# if direction == 't':
		# 	if insertion.stc.tc == self.stc.tc:
		# 		# reached temporal end, redo spatial path if possible
		# 		insertion.stc.tc = None
		# 		g.insert(insertion, ref=self)
		# 	elif insertion.stc.tc > self.stc.tc:
		# 		# Insertion still have higher temporal resolution, go deeper temporally
		# 		diff_level = insertion.stc.tc - self.stc.tc
		# 		diff_value = getattr(insertion.stc.tc, diff_level)
		# 		# Check if need to add missing t node
		# 		if diff_value not in self.t_child:
		# 			# Get current tc map, add extension, create new Node and add to child
		# 			m = self.stc.tc.map.copy()
		# 			m[diff_level] = diff_value
		# 			self.t_child[diff_value] = Node(STC(self.stc.sc, TC(m)))
		#
		# 		self.t_child[diff_value].insert(insertion, 't', g)
		#
		# else:
		# 	if insertion.stc.sc > self.stc.sc:
		# 		if len(insertion.stc.sc) - len(self.stc.sc) == 1:
		# 			# last spatial node, add ref and upgrade if necessary
		# 			if ref is not None:
		# 				ref.stc.sc
		#
		# 		# Insertion still have higher spatial resolution, go deeper spatially
		# 		diff_char = insertion.stc.sc - self.stc.sc
		#
		# 		if diff_char not in self.s_child:
		# 			self.s_child[c] = Node(STC(SC(self.stc.sc.path + c), TC(self.stc.tc)))
		#
		# 		self.s_child[c].insert(insertion, 's', g, ref)


class STGraph:

	def __init__(self):
		self.db = {}

		self.spatial_root = {}
		self.temporal_root = {}

	def retrieve(self, stc):
		print(f'retrieve: {stc}')
		if len(stc.tc) != 0:
			# retrieve from temporal roots (temporal steps are shorter)

			y = stc.tc.year
			if y in self.temporal_root:
				return self.db[self.temporal_root[y]].retrieve(stc, self.db)
			return None
		else:
			# retrieve from spatial roots:
			c = stc.sc.path[0]
			rootstc = STC(SC(c), TC())
			if c in self.spatial_root:
				return self.db[self.spatial_root[c]].retrieve(stc, self.db)
			return None

	def insert(self, insertion):
		# Start from temporal_path
		if len(insertion.stc.tc) > 0:
			y = insertion.stc.tc.year
			# Create new temporal top level node if missing
			rootstc = STC(SC(), TC({'year': y}))
			if rootstc not in self.db:
				self.temporal_root[y] = rootstc
				self.db[rootstc] = Node(rootstc)
			self.db[rootstc].insert(insertion, self.db)

		if len(insertion.stc.sc) > 0:
			c = insertion.stc.sc.path[0]
			rootstc = STC(SC(c), TC())
			if rootstc not in self.db:
				self.spatial_root[c] = rootstc
				self.db[rootstc] = Node(rootstc)
			self.db[rootstc].insert(insertion, self.db)


class FSTGraph:

	def __init__(self, features=[]):
		self.db = {feature: STGraph() for feature in features}
		self.lexer = Lexer(features)

	def add_feature(self, feature):
		if feature not in self.db:
			self.db[feature] = STGraph()
			self.lexer.features.append(feature)

	def insert(self, dt, gh, feat, val):
		insertion = self.lexer.dtghv2insertion(dt, gh, val)
		self.db[feat].insert(insertion)

	def retrieve(self, query):
		stc, feature = self.lexer.parse_query(query)
		print(stc.sc, stc.tc, feature)
		return self.db[feature].retrieve(stc)
