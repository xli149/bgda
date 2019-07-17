import sys
from runstats import Statistics
import string
import copy
import hashlib
import time
import threading


class Lexer:

	def __init__(self, features=[]):
		self.features = features

	def parse_query(self, query):
		months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
		geohash_buff = ''
		m = {}
		feature = None
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

		# find max depth
		max_depth = 0
		for i, level in enumerate(TC.levels):
			if level in m:
				max_depth = i

		for i, level in enumerate(TC.levels):
			if i < max_depth and level not in m:
				m[level] = None


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
			if getattr(self, level) is not None and getattr(other, level) is not None and getattr(self, level) != getattr(other, level):
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
		# print(f'gt({self}, {other})')
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
		return self.sc == other.sc and self.tc == other.tc

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
		self.lock = threading.Lock()
		self.spatial_root = set()
		self.temporal_root = set()

	def retrieve_root_sum(self):
		s = Statistics()
		self.lock.acquire()
		for stc in self.spatial_root:
			s += self.db[stc]

		self.lock.release()
		return s

	def retrieve(self, stc):
		self.lock.acquire()
		if stc in self.db:
			self.lock.release()
			return self.db[stc][0]

		# if there is any Nones in insertion's tc
		if None in [getattr(stc.tc, l) for l in TC.levels if hasattr(stc.tc, l)]:
			#HAS WILD CARD(S)
			if stc.tc.year == None:
				# WILD CARD @ year, rec call and sum all temporal root
				stats = Statistics()
				for trstc in self.temporal_root:
					temps = self.__retrieve_helper(trstc, stc)
					if temps is not None:
						stats += temps
				return stats if len(stats) > 0 else None
			else:
				y = insertion.stc.tc.year
				rootstc = STC(SC(), TC({'year': y}))
				if rootstc not in self.db:
					self.lock.release()
					return None
				self.__retrieve_helper(rootstc, stc)

		self.lock.release()
		return None

	def __retrieve_helper(self, curr_stc, retrieve_stc):
		print(f'__retrieve_helper({curr_stc}, {retrieve_stc})')
		if retrieve_stc == curr_stc:
			print("Check")
			return self.db[curr_stc][0]

		if retrieve_stc.tc > curr_stc.tc:
			# CHECK if next step is wild card
			diff_level, diff_value = retrieve_stc.tc - curr_stc.tc
			print(f'diff_level: {diff_level}, diff_value: {diff_value}')
			if diff_value is None:
				# Wild card, sum all temporal child
				s = Statistics()
				for stc in self.db[curr_stc][3]:
					temps = self.__retrieve_helper(stc, retrieve_stc)
					if temps is not None:
						s += temps
				return s if len(s) > 0 else None
			# Go one level deeper:
			ntc = curr_stc.tc.copy()
			setattr(ntc, diff_level, diff_value)
			ntc.depth += 1
			nstc = STC(SC(curr_stc.sc.path), ntc)
			if nstc not in self.db:
				# Doesn't have further node, no data
				return None
			return self.__retrieve_helper(nstc, retrieve_stc)
		else:
			# TC done, go sc direction
			diff_char = retrieve_stc.sc - curr_stc.sc
			nstc = STC(SC(curr_stc.sc.path + diff_char), curr_stc.tc.copy())
			if nstc not in self.db:
				# Doesn't have further node, no data
				return None
			return self.__retrieve_helper(nstc, retrieve_stc)







	def insert(self, insertion):
		# print(f'insert({insertion})')
		# Start from temporal_path
		self.lock.acquire()
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

		self.lock.release()


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
		self.features = features
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
		print(f"stc: {stc}, feature: {feature}")
		if feature is not None and str(stc) == '':
			s = Statistics()
			for r in self.db[feature].temporal_root:
				s += self.db[feature].db[r][0]
			return s

		if stc is None:
			return None

		# if no feature is specified, yet a valid stc, do featural summation
		if feature is None:
			s = Statistics()
			for feature in self.features:
				temps = self.db[feature].retrieve(stc)
				if temps is not None:
					s += temps
			return s if len(s) > 0 else None
		else:
			if feature not in self.db:
				return None
			return self.db[feature].retrieve(stc)
