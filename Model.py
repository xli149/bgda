import socket  # TODO: refactor using socketserver and handlers (maybe?)
import threading
import sys
import time


class Model:

    def __init__(self, type):
        self.type = type
        self.count = 0
        self.mean = [0]*7
        self.min = [sys.maxsize]*7
        self.max = [-sys.maxsize - 1]*7
        self.variance = [0]*7

    def update(self, record):
        self.count += 1
        a = 0
        newMax = []
        newMin = []
        newMean = []
        newVar = []
        for k, v in record.items():
            newMax.append(max(self.max[a], v))
            newMin.append(min(self.min[a], v))
            mean = self.mean[a] + (v - self.mean[a]) / self.count
            variance = self.variance[a] + (v - self.mean[a]) * (v - mean)
            newMean.append(mean)
            newVar.append(variance)
            a += 1
        self.max = newMax
        self.min = newMin
        self.mean = newMean
        self.variance = newVar
        # self.min = min(self.min, record)
        # mean = self.mean + (record - self.mean) / self.count
        # variance = self.variance + (record - self.mean) * (record - mean)
        # self.mean = mean
        # self.variance = variance

    def getMean(self):
        return self.mean

    def getVariance(self):
        return self.variance

    def getCount(self):
        return self.count

    def getMin(self):
        return self.min

    def getMax(self):
        return self.max

    def updateStats(self, recordValue):
        mean = self.mean + (recordValue - self.mean)/self.count
        variance = self.variance + (recordValue - self.mean)*(recordValue - mean)
        self.mean = mean
        self.variance = variance
