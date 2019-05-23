import threading, struct, logging, queue, time, sys, datetime

from Bin import Bin
from StreamCorrelationMatrix import StreamCorrelationMatrix
import numpy as np

import pygeohash as pgh
class DataSummarizer(threading.Thread):

    def __init__(self,queueList):
        super().__init__()
        self.queueList = queueList
        self.index = 1
        self.bins = []
        self.correlation_matrix = StreamCorrelationMatrix()
        self.geoHashList = set()
        self.featureMapping = {'AIR_TEMPERATURE':0,
                               'PRECIPITATION' :1,
                               'SOLAR_RADIATION' :2,
                               'SURFACE_TEMPERATURE' :3,
                               'RELATIVE_HUMIDITY' :4
                               }
        self.monthMapping = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    def getMaxForDay(self, day, feature):
        return self.bins[0].get(day).max[self.featureMapping[feature]]

    def getMaxStatsDaily(self, feature):
        list = []
        for i in range(0,365):
            list.append(int(self.getMaxForDay(i, feature)))
        print("here in max stats by day:" + str(list))
        return list

    def getMinForDay(self, day, feature):
        return self.bins[0].get(day).min[self.featureMapping[feature]]

    def getMinStatsDaily(self, feature):
        print("here in min daily, feature:", feature)
        list = []
        for i in range(0, 365):
            list.append(int(self.getMinForDay(i, feature)))
        print("here in max stats by day feature: " + str(list))
        return list

    def getMeanForDay(self, day, feature):
        return self.bins[0].get(day).mean[self.featureMapping[feature]]

    def getMeanStatsDaily(self, feature):
        list = []
        for i in range(0, 365):
            list.append(int(self.getMaxForDay(i, feature)))
        print("here in max stats by month feature: " + str(list))
        return list

    def getVarianceForDay(self, day, feature):
        return self.bins[0].get(day).variance[self.featureMapping[feature]]

    def getUniqueLocation(self):
        return tuple(self.geoHashList)

    def get_start_end_day_for_month(self, month):
        startDay = 0
        if month is not 1:
            for i in range(month - 1):
                startDay += self.monthMapping[i]
        endDay = startDay + self.monthMapping[month - 1] - 1
        return startDay, endDay

    def getMinForMonth(self, month, feature):
        startDay, endDay = self.get_start_end_day_for_month(month)
        min_val = 10000
        for i in range(startDay, endDay):
            value = self.getMinForDay(i, feature)
            if value != -9999:
                min_val = min(min_val, value)
        return min_val

    def getMinStatsByMonth(self, feature):

        list = []
        for i in range(1, 13):
            list.append(int(self.getMinForMonth(i, feature)))
        print("here in min stats by month feature: " + feature + str(list))
        return list

    def getMaxForMonth(self, month, feature):
        startDay, endDay = self.get_start_end_day_for_month(month)
        max_val = -1
        for i in range(startDay, endDay):
            value = self.getMaxForDay(i, feature)
            if value != -9999:
                max_val = max(max_val, value)
        return max_val


    def getMaxStatsByMonth(self, feature):
        list = []
        for i in range(1, 13):
            list.append(int(self.getMaxForMonth(i, feature)))
        print("here in max stats by month feature: " + str(list))
        return list


    def getMeanForMonth(self, month, feature):
        startDay, endDay = self.get_start_end_day_for_month(month)
        required_vals = []
        for i in range(startDay, endDay):
            value = self.getMaxForDay(i, feature)
            if value != -9999:
                required_vals.append(value)
        mean = np.mean(required_vals)
        return mean


    def getMeanStatsByMonth(self, feature):
        list = []
        for i in range(1, 13):
            list.append(int(self.getMeanForMonth(i, feature)))
        print("here in mean stats by month feature: " + str(list))
        return list

    def getStats(self, feature, statistic, resolution):
        print(feature, statistic, resolution)
        if (resolution == 'Monthly') | (resolution == 'monthly'):
            if statistic == 'min':
                return self.getMinStatsByMonth(feature)
            elif statistic == 'max':
                return self.getMaxStatsByMonth(feature)
            elif statistic == 'mean':
                return self.getMeanStatsByMonth(feature)

        elif (resolution == 'Daily') | (resolution == 'daily'):
            if statistic == 'min':
                return self.getMinStatsDaily(feature)
            elif statistic == 'max':
                return self.getMaxStatsDaily(feature)
            elif statistic == 'mean':
                return self.getMeanStatsDaily(feature)

        else:
            if statistic == 'min':
                return self.getMinStatsYearly(feature)
            elif statistic == 'max':
                return self.getMaxStatsYearly(feature)
            elif statistic == 'mean':
                return self.getMeanStatsYearly(feature)

    def getMaxStatsYearly(self, feature):
        max_val = -1
        for i in range(0, 365):
            value = self.getMaxForDay(i, feature)
            if value != -9999:
                max_val = max(max_val, value)
        return max_val

    def getMinStatsYearly(self, feature):
        min_val = 10000
        for i in range(0, 365):
            value = self.getMinForDay(i, feature)
            if value != -9999:
                min_val = min(min_val, value)
        return min_val

    def getMeanStatsYearly(self, feature):
        required_vals = []
        for i in range(0, 365):
            value = self.getMeanForDay(i, feature)
            if value != -9999:
                required_vals.append(value)
        mean_val = np.mean(required_vals)
        return mean_val

    def getVarForMonth(self, month, feature):
        startDay, endDay = self.get_start_end_day_for_month(month)
        required_vals = []
        for i in range(startDay, endDay):
            required_vals.append(self.getVarianceForDay(i, feature))
        variance = np.var(required_vals)
        return str(variance)

    def getvarStatsByMonth(self, feature):
        list = []
        for i in range(1, 13):
            list.append(self.getVarForMonth(i, feature))
        return str(list)


    def getMeanStats(self, feature):
        print("here in mean stats feature: ")
        print(feature)
        list = []
        for key in self.bins[0].keys():
            print(str(self.bins[0].get(key)) + "  for key: " + str(key))
            print("value: " + str(self.bins[0].get(key).mean[self.featureMapping[feature]]))
            list.append(self.bins[0].get(key).mean[self.featureMapping[feature]])
        print(''.join(str(x) for x in list))
        return ''.join((str(x) + "   ") for x in list)
        # return list

    def getVarStats(self, feature):
        print("here in var stats feature: ")
        print(feature)
        list = []
        for key in self.bins[0].keys():
            print(str(self.bins[0].get(key)) + "  for key: " + str(key))
            print("value: " + str(self.bins[0].get(key).variance[self.featureMapping[feature]]))
            list.append(self.bins[0].get(key).variance[self.featureMapping[feature]])
        print(''.join(str(x) for x in list))
        return ''.join((str(x) + "   ") for x in list)

    def run(self):
        print("summarizer started")

        dayBin = {i : Bin() for i in range(366)}
        locationBin = {}
        self.bins.append(dayBin)
        self.bins.append(locationBin)

        while True:
            while self.queueList.qsize() > 0:
                record = self.queueList.get()

                featureList = ['AIR_TEMPERATURE', 'PRECIPITATION', 'SOLAR_RADIATION', 'SURFACE_TEMPERATURE',
                               'RELATIVE_HUMIDITY']
                recordList = [record[i] for i in featureList]
                fmt = '%Y%m%d'
                s = str(record['UTC_DATE'])
                if s is '20180229':
                    continue
                dt = datetime.datetime.strptime(s, fmt)
                tt = dt.timetuple()
                nthDay = tt.tm_yday - 1
                dayBin[nthDay].update(recordList)

                lat = record['LATITUDE']
                long = record['LONGITUDE']
                geohash = pgh.encode(lat, long)
                if geohash in self.geoHashList:
                    locationBin[geohash].update(recordList)
                else:
                    newBin = Bin()
                    locationBin[geohash] = newBin
                    locationBin[geohash].update(recordList)
                    self.geoHashList.add(geohash)


                self.correlation_matrix.update(record)

            time.sleep(1)

