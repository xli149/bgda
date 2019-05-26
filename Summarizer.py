import threading
import struct
import logging
import queue
import time

from Model import Model

from StreamCorrelationMatrix import StreamCorrelationMatrix


class Summarizer(threading.Thread):
    def __init__(self, queueList):

        super().__init__()
        self.queueList = queueList
        self.index = 1
        self.models = []
        self.correlation_matrix = StreamCorrelationMatrix()

    def run(self):
        print("summarizer started")
        model1 = Model(1)
        model2 = Model(2)
        model3 = Model(4)
        model4 = Model(8)
        self.models.extend([model1, model2, model3, model4])

        while True:
            while not self.queueList.empty():
                record = self.queueList.get()

                for x, model in enumerate(self.models):
                    if self.index % (2 ** x) == 0:
                        model.update(record)

                self.index += 1
            time.sleep(1)

    def getStatsCount(self):
        list = []
        for x in range(4):
            list.append(self.models[x].count)
        return list

    def getStatsMax(self):
        list = []
        for x in range(4):
            list.append(self.models[x].max)
        return list

    def getStatsMin(self):
        list = []
        for x in range(4):
            list.append(self.models[x].min)
        return list

    def getStatsMean(self):
        list = []
        for x in range(4):
            list.append(self.models[x].mean)
        return list

    def getStatsVariance(self):
        list = []
        for x in range(4):
            list.append(self.models[x].variance)
        return list
# if __name__ == '__main__':
#     q = queue.Queue()
#     q.put(1)
#     q.put(2)
#     summarizer = Summarizer(q)
#     summarizer.summarize()
