import sys
from runstats import Statistics
import string
import copy


class ComparableCoordinates:
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


class SC:

	def __init__(self, spatial_path_str):
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


class TC:
	levels = ['year', 'month', 'day', 'hour']

	def __init__(self, temporal_dict):
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
		return self == other or self < other

	def __sub__(self, other):
		assert self > other
		return TC.levels[other.depth]

	def __str__(self):
		return str(self.map)



class STC:

	def __init__(self, sc, tc):
		self.sc = sc
		self.tc = tc

	def __eq__(self, other):
		if self.tc is None and other.tc is None:
			return self.sc == other.sc
		if self.sc is None and other.sc is None:
			print(self.tc)
			print(other.tc)
			return self.tc == other.tc
		return self.sc == other.sc and self.tc == other.tc


class Insertion:
	def __init__(self, stc, value):
		self.stc = stc
		self.value = value


class Node:
	ghchars = [str(i) for i in range(0, 10)] + [c for c in string.ascii_lowercase if c not in ['a', 'i', 'l', 'o']]

	def __init__(self, stc):
		self.stc = stc
		self.stats = Statistics()
		self.s_child = {}
		self.t_child = {}

	def insert(self, insertion, direction, g, ref=None):
		self.stats.push(insertion.value)
		if insertion.stc == self.stc:
			print("base case, insertion done")
			return

		if insertion.stc.tc == self.stc.tc:
			# Kick to spatial insertion if needed (with ref to self)
			pass
		if insertion.stc.sc == self.stc.sc:
			pass
			# Kick to temporal insertion if needed (with ref to self)

		if direction == 't':
			if insertion.stc.tc == self.stc.tc:
				# reached temporal end, redo spatial path if possible
				insertion.stc.tc = None
				g.insert(insertion, ref=self)
			elif insertion.stc.tc > self.stc.tc:
				# Insertion still have higher temporal resolution, go deeper temporally
				diff_level = insertion.stc.tc - self.stc.tc
				diff_value = getattr(insertion.stc.tc, diff_level)
				# Check if need to add missing t node
				if diff_value not in self.t_child:
					# Get current tc map, add extension, create new Node and add to child
					m = self.stc.tc.map.copy()
					m[diff_level] = diff_value
					self.t_child[diff_value] = Node(STC(self.stc.sc, TC(m)))

				self.t_child[diff_value].insert(insertion, 't', g)

		else:
			if insertion.stc.sc > self.stc.sc:
				if len(insertion.stc.sc) - len(self.stc.sc) == 1:
					# last spatial node, add ref and upgrade if necessary
					if ref is not None:
						ref.stc.sc

				# Insertion still have higher spatial resolution, go deeper spatially
				diff_char = insertion.stc.sc - self.stc.sc

				if diff_char not in self.s_child:
					self.s_child[c] = Node(STC(SC(self.stc.sc.path + c), TC(self.stc.tc)))

				self.s_child[c].insert(insertion, 's', g, ref)











class STSGraph:

	def __init__(self):
		self.spatial_root = {}
		self.temporal_root = {}

	def insert(self, insertion, ref=None):
		if insertion.stc.tc is None:
			# Start from spatial path:
			if insertion.stc.sc is not None:
				c = insertion.stc.sc[0]
				if c not in self.spatial_root:
					self.spatial_root[c] = Node(STC(SC(c), None))
				self.spatial_root[c].insert(insertion, 's', g, ref)
		else:
			# Start from temporal_path
			y = insertion.stc.tc.year
			# Create new temporal top level node if missing
			if y not in self.temporal_root:
				self.temporal_root[y] = Node(STC(None, TC({'year': 2019})))
			self.temporal_root[y].insert(insertion, 't', g, ref)
		# assert insertion.
