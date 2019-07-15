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
				elif seg in months:
					m['month'] = months.index(seg) + 1
				elif seg in self.features:
					feature = seg
				else:
					return None, None
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

	def copy(self):
		t = TC()
		cnt = 0
		for level in TC.levels:
			if hasattr(self, level):
				setattr(t, level, getattr(self, level))
				cnt += 1
		t.depth = cnt
		return t

class STC:
	def __init__(self, sc, tc):
		self.sc = sc
		self.tc = tc
		self.last_insertion = None

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
	def __str__(self):
		return str(self.stc) + " val: " + str(self.value)
	def __repr__(self):
		return str(self)

class STGraph:

	def __init__(self):
		# {stc: (stats, last_insertion, {stc1, stc2}, {stc3, stc4})}
		self.db = {}

		self.spatial_root = set()
		self.temporal_root = set()

	def retrieve(self, stc):
		if stc in self.db:
			print(self.db[stc][0])
			return self.db[stc][0]
		return None

	def insert(self, insertion):
		# print(f'insert({insertion})')
		# Start from temporal_path
		if len(insertion.stc.tc) > 0:
			y = insertion.stc.tc.year
			# Create new temporal top level node if missing
			rootstc = STC(SC(), TC({'year': y}))
			if rootstc not in self.db:
				self.temporal_root.add(rootstc)
				self.db[rootstc] = [Statistics(), '', set(), set()]
			self.__insert_helper(rootstc, insertion)

		if len(insertion.stc.sc) > 0:
			c = insertion.stc.sc.path[0]
			rootstc = STC(SC(c), TC())
			if rootstc not in self.db:
				self.spatial_root.add(rootstc)
				self.db[rootstc] = [Statistics(), '', set(), set()]
			self.__insert_helper(rootstc, insertion)

	def __insert_helper(self, stc, insertion):
		# print(f'__insert_helper({stc}, {insertion})')
		self.db[stc][1] = insertion.hash
		self.db[stc][0].push(insertion.value)
		if insertion.stc == stc:
			# done base case, no need further
			return

		if insertion.stc.tc > stc.tc:
			diff_level, diff_value = insertion.stc.tc - stc.tc
			# print(f'diff_level: {diff_level}, diff_value: {diff_value}')
			ntc = stc.tc.copy()
			setattr(ntc, diff_level, diff_value)
			ntc.depth += 1
			nstc = STC(SC(stc.sc.path), ntc)
			# print(f'nstc: {nstc}')
			if nstc not in self.db:
				# Create the new stc
				self.db[nstc] = [Statistics(), '', set(), set()]
				# Insert new stc to old stc's child
				self.db[stc][3].add(nstc)

			if self.db[nstc][1] != insertion.hash:
				self.__insert_helper(nstc, insertion)

		if insertion.stc.sc > stc.sc:
			diff_char = insertion.stc.sc - stc.sc
			nstc = STC(SC(stc.sc.path + diff_char), stc.tc.copy())
			if nstc not in self.db:
				self.db[nstc] = [Statistics(), '', set(), set()]
				self.db[stc][2].add(nstc)
			if self.db[nstc][1] != insertion.hash:
				self.__insert_helper(nstc, insertion)


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
		if feature is None or feature not in self.db:
			return None
		return self.db[feature].retrieve(stc)
