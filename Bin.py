import socket  # TODO: refactor using socketserver and handlers (maybe?)
import threading
import sys
import time
import datetime


class Bin:

    def __init__(self):
        self.size = 5 # represent each of the 5 features
        self.count = 0
        self.mean = [0]*self.size
        self.min = [100000]*self.size
        self.max = [-1]*self.size
        self.variance = [0]*self.size

    def update(self, record_list):

        self.count += 1
        self.max = [max(x1, x2) for x1, x2 in zip(self.max, record_list)]
        self.min = [min(x1, x2) for x1, x2 in zip(self.min, record_list)]

        for i in range(self.size):
            mean = self.mean[i] + (record_list[i] - self.mean[i]) / self.count
            variance = self.variance[i] + (record_list[i] - self.mean[i]) * (record_list[i] - mean)
            self.mean[i] = mean
            self.variance[i] = variance/self.count  # TODO: Check if this formula works!




