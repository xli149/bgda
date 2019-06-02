import threading
import struct
import logging
import queue
import time
import sys
import datetime

from Bin import Bin
from FeatureBin import FeatureBin
from StreamCorrelationMatrix import StreamCorrelationMatrix
import numpy as np

import pygeohash as pgh


class DataSummarizer(threading.Thread):

    def __init__(self, queue_list):
        super().__init__()
        self.queueList = queue_list
        self.geoHashList = set()
        # TODO: Need to load this feature_list else where so all components agree on the same list
        self.feature_list = ['AIR_TEMPERATURE', 'PRECIPITATION', 'SOLAR_RADIATION', 'SURFACE_TEMPERATURE',
                        'RELATIVE_HUMIDITY']
        # Initialize len(featureMapping) amount of feature bins
        self.bins = {f: FeatureBin(f) for f in self.feature_list}
        self.correlation_matrix = StreamCorrelationMatrix()

        self.monthMapping = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    def get_max_for_day(self, day, feature):
        return self.bins[feature].days_stats[day].maximum()

    def get_max_stats_daily(self, feature):
        list = []
        for i in range(0, 365):
            list.append(int(self.get_max_for_day(i, feature)))
        print("here in max stats by day:" + str(list))
        return list

    def get_min_for_day(self, day, feature):
        return self.bins[feature].days_stats[day].minimum()

    def get_min_stats_daily(self, feature):
        print("here in min daily, feature:", feature)
        list = []
        for i in range(0, 365):
            list.append(int(self.get_min_for_day(i, feature)))
        print("here in max stats by day feature: " + str(list))
        return list

    def get_mean_for_day(self, day, feature):
        return self.bins[feature].days_stats[day].mean()

    def get_mean_stats_daily(self, feature):
        list = []
        for i in range(0, 365):
            list.append(int(self.get_max_for_day(i, feature)))
        print("here in max stats by month feature: " + str(list))
        return list

    def get_variance_for_day(self, day, feature):
        return self.bins[feature].days_stats[day].variance()

    def get_unique_location(self):
        return tuple(self.geoHashList)

    def get_start_end_day_for_month(self, month):
        start_day = 0
        if month is not 1:
            for i in range(month - 1):
                start_day += self.monthMapping[i]
        end_day = start_day + self.monthMapping[month - 1] - 1
        return start_day, end_day

    def get_min_for_month(self, month, feature):
        startDay, endDay = self.get_start_end_day_for_month(month)
        min_val = 10000
        for i in range(startDay, endDay):
            value = self.get_min_for_day(i, feature)
            if value != -9999:
                min_val = min(min_val, value)
        return min_val

    def get_min_stats_by_month(self, feature):

        list = []
        for i in range(1, 13):
            list.append(int(self.get_min_for_month(i, feature)))
        print("here in min stats by month feature: " + feature + str(list))
        return list

    def get_max_for_month(self, month, feature):
        startDay, endDay = self.get_start_end_day_for_month(month)
        max_val = -1
        for i in range(startDay, endDay):
            value = self.get_max_for_day(i, feature)
            if value != -9999:
                max_val = max(max_val, value)
        return max_val

    def get_max_stats_by_month(self, feature):
        list = []
        for i in range(1, 13):
            list.append(int(self.get_max_for_month(i, feature)))
        print("here in max stats by month feature: " + str(list))
        return list

    def get_mean_for_month(self, month, feature):
        start_day, end_day = self.get_start_end_day_for_month(month)
        required_vals = []
        for i in range(start_day, end_day):
            value = self.get_max_for_day(i, feature)
            if value != -9999:
                required_vals.append(value)
        mean = np.mean(required_vals)
        return mean

    def get_mean_stats_by_month(self, feature):
        list = []
        for i in range(1, 13):
            list.append(int(self.get_mean_for_month(i, feature)))
        print("here in mean stats by month feature: " + str(list))
        return list

    def get_stats(self, feature, statistic, resolution):
        print(feature, statistic, resolution)
        if (resolution == 'Monthly') | (resolution == 'monthly'):
            if statistic == 'min':
                return self.get_min_stats_by_month(feature)
            elif statistic == 'max':
                return self.get_max_stats_by_month(feature)
            elif statistic == 'mean':
                return self.get_mean_stats_by_month(feature)

        elif (resolution == 'Daily') | (resolution == 'daily'):
            if statistic == 'min':
                return self.get_min_stats_daily(feature)
            elif statistic == 'max':
                return self.get_max_stats_daily(feature)
            elif statistic == 'mean':
                return self.get_mean_stats_daily(feature)

        else:
            if statistic == 'min':
                return self.get_min_stats_yearly(feature)
            elif statistic == 'max':
                return self.get_max_stats_yearly(feature)
            elif statistic == 'mean':
                return self.get_mean_stats_yearly(feature)

    def get_max_stats_yearly(self, feature):
        max_val = -1
        for i in range(0, 365):
            value = self.get_max_for_day(i, feature)
            if value != -9999:
                max_val = max(max_val, value)
        return max_val

    def get_min_stats_yearly(self, feature):
        min_val = 10000
        for i in range(0, 365):
            value = self.get_min_for_day(i, feature)
            if value != -9999:
                min_val = min(min_val, value)
        return min_val

    def get_mean_stats_yearly(self, feature):
        required_vals = []
        for i in range(0, 365):
            value = self.get_mean_for_day(i, feature)
            if value != -9999:
                required_vals.append(value)
        mean_val = np.mean(required_vals)
        return mean_val

    def get_var_for_month(self, month, feature):
        startDay, endDay = self.get_start_end_day_for_month(month)
        required_vals = []
        for i in range(startDay, endDay):
            required_vals.append(self.get_variance_for_day(i, feature))
        variance = np.var(required_vals)
        return str(variance)

    def getvar_stats_by_month(self, feature):
        list = []
        for i in range(1, 13):
            list.append(self.get_var_for_month(i, feature))
        return str(list)

    def get_mean_stats(self, feature):
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

    def get_var_stats(self, feature):
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
        print("DataSummarizer started")
        fmt = '%Y%m%d'
        while True:
            while self.queueList.qsize() > 0:
                record = self.queueList.get()
                s = str(record['UTC_DATE'])
                if s is '20180229':
                    continue
                dt = datetime.datetime.strptime(s, fmt)
                tt = dt.timetuple()
                nth_day = tt.tm_yday - 1
                for f in self.feature_list:
                    if f in record:
                        self.bins[f].update(record[f], nth_day)

                # TODO: These will be removed in distributed Aggregator
                # lat = record['LATITUDE']
                # long = record['LONGITUDE']
                # geohash = pgh.encode(lat, long)
                # if geohash not in self.geoHashList:
                #     new_bin = Bin()
                #     location_bin[geohash] = new_bin
                #     self.geoHashList.add(geohash)
                # location_bin[geohash].update(record_list)
                self.correlation_matrix.update(record)

            time.sleep(1)
