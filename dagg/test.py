
from ght import GeohashTrie
import datetime

features = ['FA', 'FB']
ght = GeohashTrie('9', features)


tm = datetime.datetime.strptime("06/14/2019","%m/%d/%Y")
ght.insert('92', 'FA', 2, tm)
ght.insert('92', 'FA', 3, tm)

# show me the mean of feature FA of june 2019 under the gh zone '92'
print(ght.get('92', 'FA').months_stats[5].mean() == 2.5)


ght.insert('9255', 'FB', 5, tm)
ght.insert('9255', 'FA', 4, tm)


# show me the mean of feature FA of june 2019 under the gh zone '92'
print(ght.get('92', 'FA').months_stats[5].mean() == 3)
# show me the number of records of feature FB of june 2019 under gh zone 9
print(len(ght.get('9', 'FB').months_stats[5]))
# show me the number of records of feature FA of june 2019 under gh zone 9
print(len(ght.get('9', 'FA').months_stats[5]))
