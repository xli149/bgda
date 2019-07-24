import sys
from runstats.fast import Statistics
import string
import copy
import hashlib
import time
import threading
import queue
import pygeohash as pgh
import datetime
import math
import collections
import statistics
import random



class Lexer:

    def __init__(self, features=[]):
        self.features = features

    def parse_query(self, query):
        months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
        geohash_buff = ''
        m = {}
        feature = None
        if query.strip() == '':
            return STC(SC(''), TC({})), None
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

        geohash_buff = geohash_buff[:min(6, len(geohash_buff))]

        return STC(SC(geohash_buff), TC(m)), feature

    def dtghv2insertion(dt, gh, val):
        m = {}
        m['year'] = dt.year
        m['month'] = dt.month
        m['day'] = dt.day
        m['hour'] = dt.hour
        gh = gh[:min(6, len(gh))]

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

class STGraph():

    def __init__(self, feature):
        # {stc: (stats, last_insertion, {stc1, stc2}, {stc3, stc4})}
        self.db = {}
        self.feature = feature
        self.lock = threading.Lock()
        self.spatial_root = set()
        self.temporal_root = set()
        self.queue = queue.Queue()
        self.thread = threading.Thread(target=self.run)
        self.rdeque = collections.deque(maxlen=10000)
        self.thread.start()

    def run(self):
        dfmt = '%Y%m%d'
        while True:
            while self.queue.qsize() > 0:
                self.rdeque.appendleft(datetime.datetime.now())
                record = self.queue.get()
                gh = pgh.encode(record['LATITUDE'], record['LONGITUDE'])
                t = record['UTC_TIME']
                dt = datetime.datetime.strptime(str(record['UTC_DATE']), dfmt)
                dt = dt.replace(hour=t//100, minute=t%100)
                insertion = Lexer.dtghv2insertion(dt, gh, record[self.feature])
                self.__insert(insertion)
            time.sleep(1)

    def rpm(self):
        one_min_ago = datetime.datetime.now() - datetime.timedelta(minutes=1)
        total = 0
        for t in self.rdeque:
            if t > one_min_ago:
                total += 1
            else:
                break
        return total

    def qsize(self):
        return self.queue.qsize()


    def retrieve_root_sum(self):
        s = Statistics()
        m = {}
        self.lock.acquire()
        for stc in self.spatial_root:
            s += self.db[stc][0]
            m[stc] = len(self.db[stc][0])

        for stc in self.temporal_root:
            m[stc] = len(self.db[stc][0])
        self.lock.release()
        return s, collections.Counter(m)

    def lower_distr(self, stc):
        m = {}
        for sc in self.db[stc][2]:
            m[sc] = len(self.db[sc][0])

        for tc in self.db[stc][3]:
            m[tc] = len(self.db[tc][0])

        return collections.Counter(m)

    def retrieve(self, stc):
        self.lock.acquire()
        if stc in self.db:
            v = self.db[stc][0]
            self.lock.release()
            return v, self.lower_distr(stc)

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
                self.lock.release()
                return stats if len(stats) > 0 else None, None
            else:
                y = stc.tc.year
                rootstc = STC(SC(), TC({'year': y}))
                if rootstc not in self.db:
                    self.lock.release()
                    return None, None
                v = self.__retrieve_helper(rootstc, stc)
                self.lock.release()
                return v, None

        self.lock.release()
        return None, None

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
                    print('checkkk')
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

    def insert(self, record):
        self.queue.put(record)


    def __insert(self, insertion):
        # print(f'insert({insertion})')
        # Start from temporal_path
        self.lock.acquire()
        if len(insertion.stc.tc) > 0:
            # Create new temporal top level node if missing
            rootstc = STC(SC(), TC({'year': insertion.stc.tc.year}))
            if rootstc not in self.db:
                self.temporal_root.add(rootstc)
                self.db[rootstc] = [Statistics(), '', set(), set()]
            self.__insert_helper(rootstc, insertion)
        if len(insertion.stc.sc) > 0:
            rootstc = STC(SC(insertion.stc.sc.path[0]), TC())
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
        self.ringsize = 5
        self.db = {feature: [STGraph(feature) for _ in range(self.ringsize)] for feature in features}
        self.features = features
        self.lexer = Lexer(features)


    def rpm(self):
        return statistics.mean([sum([stg.rpm() for stg in ring]) for ring in self.db.values()])

    def qsize(self):
        # return sum([stg.rpm() for ring in self.db.values() for stg in ring])
        return statistics.mean([sum([stg.qsize() for stg in ring]) for ring in self.db.values()])

    def qsizebf(self, feature):
        return [stg.qsize() for stg in self.db[feature]]

    def insert(self, record):
        for feature in self.features:
            # FIXME: Temporary removal of -9999 until stddev is ready
            if feature in record and record[feature] != -9999:
                random.choice(self.db[feature]).insert(record)


    def retrieve(self, query):
        stc, feature = self.lexer.parse_query(query)
        print(f"stc: {stc}, feature: {feature}")
        if feature is not None and str(stc) == '':
            # Only provided feature, sum this feature's root
            s = Statistics()
            c = collections,Counter({})
            for stg in self.db[feature]:
                temps, tempc = stg.retrieve_root_sum()
                if temps is not None:
                    s += temps
                if tempc is not None:
                    c += tempc
            return s, c

        if feature is None and str(stc) == '':
            # Nothing provided, sum everything
            s = Statistics()
            c = collections.Counter({})
            for feature in self.features:
                for stg in self.db[feature]:
                    temps, tempc = stg.retrieve_root_sum()
                    if temps is not None:
                        s += temps
                    if tempc is not None:
                        c += tempc
            return s, c


        if stc is None:
            return None, None

        # if no feature is specified, yet a valid stc, do featural summation
        if feature is None:
            s = Statistics()
            c = collections.Counter({})
            for feature in self.features:
                for stg in self.db[feature]:
                    temps, tempc = stg.retrieve(stc)
                    if temps is not None:
                        s += temps
                    if tempc is not None:
                        c += tempc
            return s if len(s) > 0 else None, c if len(s) > 0 else None
        else:
            if feature not in self.db:
                return None, None
            s = Statistics()
            c = collections.Counter({})

            for stg in self.db[feature]:
                temps, tempc = stg.retrieve(stc)
                if temps is not None:
                    s += temps
                if tempc is not None:
                    c += tempc
            return s if len(s) > 0 else None, c if len(s) > 0 else None
