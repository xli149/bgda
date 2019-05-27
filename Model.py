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
        new_max = []
        new_min = []
        new_mean = []
        new_var = []
        for k, v in record.items():
            new_max.append(max(self.max[a], v))
            new_min.append(min(self.min[a], v))
            mean = self.mean[a] + (v - self.mean[a]) / self.count
            variance = self.variance[a] + (v - self.mean[a]) * (v - mean)
            new_mean.append(mean)
            new_var.append(variance)
            a += 1
        self.max = new_max
        self.min = new_min
        self.mean = new_mean
        self.variance = new_var
        # self.min = min(self.min, record)
        # mean = self.mean + (record - self.mean) / self.count
        # variance = self.variance + (record - self.mean) * (record - mean)
        # self.mean = mean
        # self.variance = variance

    def get_mean(self):
        return self.mean

    def get_variance(self):
        return self.variance

    def get_count(self):
        return self.count

    def get_min(self):
        return self.min

    def get_max(self):
        return self.max

    def update_stats(self, record_value):
        mean = self.mean + (record_value - self.mean) / self.count
        variance = self.variance + (record_value - self.mean) * (record_value - mean)
        self.mean = mean
        self.variance = variance
