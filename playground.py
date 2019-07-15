from FSTGraph import *
import datetime

s1 = SC("93f")
s2 = SC("93fcea")
s3 = SC('')

t1 = TC({
	'year': 2019,
	'month': 6,
	'day': 15
})
t2 = TC({
	'year': 2019,
	'month': 6
})
t3 = TC()

# Test SC arithmetics
assert len(s1) == 3
assert len(s2) == 6
assert len(s3) == 0
assert s2 - s3 == '9'
assert s1 < s2
assert s2 > s1
assert s3 != s2
assert s1 >= s3
# Test TC arithmetics
assert len(t1) == 3
assert len(t2) == 2
assert len(t3) == 0
assert t1 != t2
assert t1 != t3
assert t3 < t1
assert t1 >= t2
assert t1 - t2 == ('day', 15)

i1 = Insertion(STC(s2, t1), 5)
i2 = Insertion(STC(s2, t2), 10)

g = STGraph()
g.insert(i1)
g.insert(i2)

# test sample sizes
assert len(g.retrieve(STC(s2, TC({'year': 2019})))) == 2
assert len(g.retrieve(STC(s2, t2))) == 2
assert len(g.retrieve(STC(s2, t1))) == 1
assert len(g.retrieve(STC(s1, TC()))) == 2
assert len(g.retrieve(STC(s1, t2))) == 2
assert len(g.retrieve(STC(s1, t1))) == 1

# test statistics
stat1 = g.retrieve(STC(s2, TC({'year': 2019})))
assert stat1.mean() == 7.5
assert stat1.maximum() == 10
assert stat1.minimum() == 5

stat2 = g.retrieve(STC(s1, t1))
assert stat2.mean() == 5
assert stat2.maximum() == 5
assert stat2.minimum() == 5







# Multiple insertion by hours test
g = STGraph()
entries = []
now = datetime.datetime.strptime('2019/06/15/11', '%Y/%m/%d/%H')
delta = datetime.timedelta(hours=1)

for i in range(50):
	now += delta
	m = {
		'year': now.year,
		'month': now.month,
		'day': now.day,
		'hour': now.hour
	}
	stc = STC(SC('93ef'), TC(m))
	i = Insertion(stc, now.day)
	entries.append(i)
	g.insert(i)

assert len(g.db['2019'][0]) == 50


fst = FSTGraph(['fa', 'fb'])
fst.insert(datetime.datetime.now(), 'abc', 'fa', 10)
