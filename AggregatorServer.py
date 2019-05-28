import socket  # TODO: refactor using socketserver and handlers (maybe?)
import threading
import queue
import sys
import time
from StreamWorker import StreamWorker

from DataSummarizer import DataSummarizer
import datetime

from xmlrpc.server import SimpleXMLRPCServer


class AggregatorServer(threading.Thread):

    def __init__(self, host, port):
        super().__init__()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((host, port))
        self.streams = []  # TODO: implement logic for removing dead streams from this list
        self.host = host
        self.port = port
        self.index = {'records_observed': 0, 'connections_made': 0}
        self.start_time = time.time()
        self.queueList = queue.Queue()
        self.summarizer = DataSummarizer(self.queueList)

    def run(self):
        # consider starting up the Command Line Interpreter here?
        print(f"server listening on {self.host}:{self.port}\n")
        with self.server_socket as s:
            s.listen()

            self.summarizer.start()
            while True:
                client_socket, address = s.accept()
                print(f"connection opened by {address}")
                self.index['connections_made'] += 1
                stream = StreamWorker(client_socket, address, self.index, self.queueList)

                self.streams.append(stream)
                stream.start()

    def start_interpreter(self):
        alive = True

        while alive:
            print("> ", end='')
            line = input()
            command = line.split(" ", 1)[0]

            if command == "ls":
                [print(stream, stream.is_alive()) for stream in self.streams]
            elif command == 'info':
                print(f"aggregator server started {self.start_time}")
                print(f"{self.index['connections_made']} connections opened.")
                print(f"{self.index['records_observed']} records processed.")
            elif command == 'exit':
                print('goodbye')
                sys.exit(0)
            elif command == 'help':
                print('print help message')  # TODO: write this up
            elif command == 'getCount':
                print("Press 0 for Day and 1 for Month")
                line = input()
                self.print_stats(line.split(" ", 1)[0], 1)
            elif command == 'getMax':
                print("Press 0 for Day and 1 for Month")
                line = input()
                self.print_stats(line.split(" ", 1)[0], 2)
            elif command == 'getMin':
                print("Press 0 for Day and 1 for Month")
                line = input()
                self.print_stats(line.split(" ", 1)[0], 3)
            elif command == 'getMean':
                print("Press 0 for Day and 1 for Month")
                line = input()
                self.print_stats(line.split(" ", 1)[0], 4)
            elif command == 'getVariance':
                print("Press 0 for Day and 1 for Month")
                line = input()
                self.print_stats(line.split(" ", 1)[0], 5)
            elif command == 'corr':
                a1 = line.split(" ")[1]
                a2 = line.split(" ")[2]
                print(self.summarizer.correlation_matrix.get_correlation(a1, a2))
            else:
                print(f"command: {command} not supported. try help")

    def print_stats(self, resolution_level, stat_variable):
        # print("resolutionlevel: " + str(resolution_level) + " and statsVariable: " + str(stat_variable))
        if int(resolution_level) == 1:
            print("Enter the month number: ")
            line = input()
            month = line.split(" ", 1)[0]
            if self.summarizer.bins[int(resolution_level)].count[int(month) - 1] == 0:
                print("No records found for this day!")
            elif stat_variable == 1:
                print("The number of records processed at this month are: " +
                      str(self.summarizer.bins[int(resolution_level)].count[int(month) - 1]))
            elif stat_variable == 2:
                print("The maximum of all records processed at this month are: " +
                      str(self.summarizer.bins[int(resolution_level)].max[int(month) - 1]))
            elif stat_variable == 3:
                print("The minimum of all records processed at this month are: " +
                      str(self.summarizer.bins[int(resolution_level)].min[int(month) - 1]))
            elif stat_variable == 4:
                print("The mean of records processed at this month are: " +
                      str(self.summarizer.bins[int(resolution_level)].mean[int(month) - 1]))
            elif stat_variable == 5:
                print("The variance of records processed at this month are: " +
                      str(self.summarizer.bins[int(resolution_level)].variance[int(month) - 1]))

        elif int(resolution_level) == 0:
            print("Enter the day(yyyymmdd): ")
            line = input()
            day = line.split(" ", 1)[0]
            dayValue = self.nth_day_of_year(day)
            print("dayvalue: " + str(dayValue))

            if self.summarizer.bins[int(resolution_level)].count[int(dayValue) - 1] == 0:
                print("No records found for this day!")
                'break'
            elif stat_variable == 1:
                print("The number of records processed on this day are: " +
                      str(self.summarizer.bins[int(resolution_level)].count[int(dayValue) - 1]))
            elif stat_variable == 2:
                print("The max of records processed on this day are: " +
                      str(self.summarizer.bins[int(resolution_level)].max[int(dayValue) - 1]))
            elif stat_variable == 3:
                print("The min of records processed on this day are: " +
                      str(self.summarizer.bins[int(resolution_level)].min[int(dayValue) - 1]))
            elif stat_variable == 4:
                print("The mean of records processed on this day are: " +
                      str(self.summarizer.bins[int(resolution_level)].mean[int(dayValue) - 1]))
            elif stat_variable == 5:
                print("The variance of records processed on this day are: " +
                      str(self.summarizer.bins[int(resolution_level)].variance[int(dayValue) - 1]))

        else:
            print("here")

    def nth_day_of_year(self, day):
        fmt = '%Y%m%d'
        dt = datetime.datetime.strptime(day, fmt)
        tt = dt.timetuple()
        index = tt.tm_yday
        return index


if __name__ == '__main__':
    agg_server = AggregatorServer('localhost', 55555)
    agg_server.start()
    # agg_server.start_interpreter()

    rpc_server = SimpleXMLRPCServer(("localhost", 2222))
    # rpc_server.register_function( "get_correlation")
    rpc_server.register_instance(agg_server, allow_dotted_names=True)
    rpc_server.serve_forever()
