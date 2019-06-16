from runstats import Statistics, Regression
from FeatureBin import FeatureBin
from RegressionMatrix import LinearRegressionMatrix
import string

# A node of the geohash trie, each contains the geohash character,
# the prefix from root, and a reference to parent node

class GHTNode():

	ghchars = [str(i) for i in range(0, 10)] + [c for c in string.ascii_lowercase if c not in ['a', 'i', 'l', 'o']]

	def __init__(self, ghchar, prefix, parent, features):
		self.ghchar = ghchar
		self.prefix = prefix
		self.parent = parent
		self.features = features

class RealNode(GHTNode):
	def __init__(self, *args):
		GHTNode.__init__(self, *args)
		self.bins = {f: FeatureBin(f) for f in self.features}
		# print(self.bins)
		self.regressionMatrix = LinearRegressionMatrix()
		self.child = {}

	def insert(self, prefix, feature, val, tm):
		# print(f'insert({prefix}, {feature}, {val}, {tm})')
		# Do nothing if prefix = ""
		if len(prefix) == 0:
			return
		# if prefix matches the char of current node, update

		if prefix[0] == self.ghchar:
			self.bins[feature].update(val, tm)
			if len(prefix) > 1:
				c = prefix[1]
				# Has more characters, propagate to child
				if c not in GHTNode.ghchars:
					return
				# Create new child node if does not exist
				if c not in self.child:
					# print(f'RealNode({c}, {self.prefix + c}, {self}, {self.features})')
					self.child[c] = RealNode(c, self.prefix + c, self, self.features)
				self.child[c].insert(prefix[1:], feature, val, tm)


	def get(self, prefix, feature):
		# print(f'self.ghchar = {self.ghchar}: get({prefix}, {feature})')

		if prefix == self.ghchar:
			return self.bins[feature]

		if len(prefix) > 1 and prefix[0] == self.ghchar:
			return self.child[prefix[1]].get(prefix[1:], feature) if prefix[1] in self.child else None


class GeohashTrie():

	def __init__(self, root_gh, features):
		self.root_gh = root_gh
		self.root = RealNode(root_gh[-1], root_gh, None, features)
		self.features = features

	def insert(self, gh, feature, val, tm):
		if not gh.startswith(self.root_gh):
			return

		self.root.insert(gh, feature, val, tm)


	def get(self, gh, feature):
		if not gh.startswith(self.root_gh):
			return None
		fb = self.root.get(gh, feature)

		return fb
