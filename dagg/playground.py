from statsdag import *

s1 = SC("93f")
s2 = SC("93fcea")


print(s1 - s2)


t1 = TC({
	'year': 2019,
	'month': 6
})
t2 = TC({
	'year': 2019,
	'month': 6,
	'day': 15
})


print(t2 - t1)

# i1 = Insertion(STC(SC('93f'), TC({
# 	'year': 2019,
# 	'month': 6,
# 	'day': 15,
# 	'hour': 1
# })), 5)
# i2 = Insertion(STC(None, TC({
# 	'year': 2019,
# 	'month': 6,
# 	'day': 16,
# 	'hour': 7
# })), 10)
#
#
# g = STSGraph()
# g.insert(i1)
