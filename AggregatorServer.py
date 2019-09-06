import socket  # TODO: refactor using socketserver and handlers (maybe?)
import threading
import queue
import sys
import time
import pickle
import json
from StreamWorker import StreamWorker

from DataSummarizer import DataSummarizer
from calendar import monthrange
import datetime
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
from transport import RequestsTransport
from FSTGraph import Lexer
from runstats.fast import Statistics
import collections


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
        self.feature_list = ['AIR_TEMPERATURE', 'PRECIPITATION', 'SOLAR_RADIATION', 'SURFACE_TEMPERATURE', 'RELATIVE_HUMIDITY']
        self.summarizer = DataSummarizer(self.queueList, self.feature_list)
        self.nodes_assignment = {f: [] for f in self.feature_list}
        self.lexer = Lexer(self.feature_list)


    def register_node(self, ip, port):
        # determine which feature this node should be responsible for
        f = self.get_next_assignment_feature()
        print(f"assigned node {ip} and port {port} to handle {f}")

        # open socket to this node
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_addr = (ip, port)
        client_socket.connect(server_addr)

        # create message queue to this node
        # this is needed because register_node will be called from multiple StreamWorker
        # but sockets are not thread safe, so StreamWorker will add to queue, and a
        # separate thread will be responsible for sending
        q = queue.Queue()

        # create thread to consume this queue,
        t = threading.Thread(target=AggregatorServer.sender_thread, args=(q, client_socket, server_addr))
        t.daemon = True
        t.start()

        # create corresponded proxies for rpc
        node_proxy = ServerProxy(f'http://{ip}:{port + 1000}/', transport=RequestsTransport(), allow_none=True)

        # register the new node
        self.nodes_assignment[f].append((ip, port, q, t, node_proxy))

        return f


    def sender_thread(queue, client_socket, server_addr):
        while True:
            while not queue.empty():
                (size, message) = queue.get()
                # send length
                client_socket.sendto(size, server_addr)
                # send message
                client_socket.sendto(message, server_addr)

    def get_next_assignment_feature(self):
        # find the feature with minimum node count
        return min([f for f in self.nodes_assignment], key=lambda f: len(self.nodes_assignment[f]))

    def assignment(self):
        result_dict = {}
        for feature in self.nodes_assignment:
            feature_dict = {}
            for node in self.nodes_assignment[feature]:
                feature_dict[node[0] + ":" + str(node[1])] = {'rr': node[4].rr(), 'rrpm': node[4].rrpm()}
            result_dict[feature] = feature_dict
        return result_dict



    def execute(self, query):
        stc, feature = self.lexer.parse_query(query)
        print(f"stc: {stc}, feature: {feature}")

        s = Statistics()
        c = collections.Counter({})

        lock_s = threading.Lock()
        lock_c = threading.Lock()

        def __exec_node(node):
            nonlocal s, c

            proxy = node[4]
            b = bytes.fromhex(proxy.retrieve(stc))
            print(f'received type {type(b)}: {b} from node')
            (temps, tempc) = pickle.loads(b)
            if temps:
                lock_s.acquire()
                s += temps
                lock_s.release()
            if tempc:
                lock_c.acquire()
                c += tempc
                lock_c.release()


        if stc is None:
            return pickle.dumps((None, None)).hex()

        # If featural summation
        threads = []
        if feature == None:
            for feature in self.nodes_assignment:
                for node in self.nodes_assignment[feature]:
                    t = threading.Thread(target=__exec_node(node))
                    threads.append(t)
                    t.start()
            for t in threads:
                t.join()
            return pickle.dumps((s, c)).hex()

        for node in self.nodes_assignment[feature]:
            t = threading.Thread(target=__exec_node(node))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()
        return pickle.dumps((s, c)).hex()

    def run(self):
        print(f"server listening on {self.host}:{self.port}")
        self.summarizer.start()
        threading.Thread(target=self.start_interpreter).start()
        with self.server_socket as s:
            s.listen()
            while True:
                client_socket, address = s.accept()
                print(f"connection opened by {address}")
                self.index['connections_made'] += 1
                stream = StreamWorker(client_socket, address, self.index, self)
                self.streams.append(stream)
                stream.start()

    def start_interpreter(self):
        alive = True
        print("Aggregator CLI started, use command 'help' for more details")
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
                # TODO: Now that start_interpreter is running in a seperate thread,
                # sys.exit(0) will only exit the interpreter thread and not shutting down the aggregator server
                print('goodbye')
                sys.exit(0)
            elif command == 'help':
                print('commands:')
                print('  ls\t\tdisplay a list of all incoming streams')
                print('  info\t\tdisplay information of the aggregator server')
                print('  getCount\tdisplay the number of records of the given resolution and date interval')
                print('  getMax\tdisplay the maximum value of the given resolution and date interval')
                print('  getMin\tdisplay the minimum value of the given resolution and date interval')
                print('  getMean\tdisplay the mean value of the given resolution and date interval')
                print('  getVariance\tdisplay the variance of the given resolution and date interval')
                print('  corr <attr1> <attr2>\tdisplay the correlation matrix of two attributes')
                print('  exit\t\tshutdown the aggregator server')
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
            elif command == 'exec':
                q = line.split(" ")[1]
                stats = self.execute(q)
                if stats is None:
                    print(None)
                else:
                    print(self.stats2json(stats))
            elif command == 'as':
                print(json.dumps(self.assignment(), indent=4))
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

    def stats2json(self, stats):

        m = {}
        if stats[0] is None:
            return m
        print(stats)
        m['size'] = len(stats[0])
        m['max'] = stats[0].maximum()
        m['mean'] = stats[0].mean()
        m['min'] = stats[0].minimum()
        m['stdev'] = stats[0].stddev()
        m['var'] = stats[0].variance()
        if stats[1]:
            m['distr'] = {str(k):v for k, v in dict(stats[1]).items()}

        return json.dumps(m, indent=4)


if __name__ == '__main__':
    agg_server = AggregatorServer('localhost', 55555)
    agg_server.start()
    # agg_server.start_interpreter()

    rpc_server = SimpleXMLRPCServer(("localhost", 2222), allow_none=True)
    # rpc_server.register_function( "get_correlation")
    rpc_server.register_instance(agg_server, allow_dotted_names=True)
    rpc_server.serve_forever()
