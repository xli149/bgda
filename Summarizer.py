import threading
import struct
import logging
import queue
import time

from Model import Model

from StreamCorrelationMatrix import StreamCorrelationMatrix


class Summarizer(threading.Thread):
    def __init__(self, queue_list):

        super().__init__()
        self.queue_list = queue_list
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
            while not self.queue_list.empty():
                record = self.queue_list.get()

                for x, model in enumerate(self.models):
                    if self.index % (2 ** x) == 0:
                        model.update(record)

                self.index += 1
            time.sleep(1)

    def get_stats_count(self):
        list = []
        for x in range(4):
            list.append(self.models[x].count)
        return list

    def get_stats_max(self):
        list = []
        for x in range(4):
            list.append(self.models[x].max)
        return list

    def get_stats_min(self):
        list = []
        for x in range(4):
            list.append(self.models[x].min)
        return list

    def get_stats_mean(self):
        list = []
        for x in range(4):
            list.append(self.models[x].mean)
        return list

    def get_stats_variance(self):
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
