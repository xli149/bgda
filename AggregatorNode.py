import json
import struct
import click
import socket
import signal
import xmlrpc.client
import threading
import collections
import traceback
import random
import datetime
import os
import sys
import pickle
import multiprocessing
from transport import RequestsTransport
from FSTGraph import STGraph, STC
from xmlrpc.server import SimpleXMLRPCServer
from runstats.fast import Statistics

class AggregatorProcess():

    def __init__(self, conn, query_queue, feature):
        self.conn = conn
        self.feature = feature
        self.query_queue = query_queue

    def sigterm_handler(self, _1, _2):
        sys.exit(0)

    def run(self):
        self.stg = STGraph(self.feature)
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        while True:
            # Prioritize queries every cycle
            if not self.query_queue.empty():
                # print(self.stg.db)
                stc = self.query_queue.get()
                print(f"received query : {str(stc)}")
                result = self.stg.retrieve(stc)
                print(f"got result: {result}")
                self.conn.send(result)
                continue

            try:
                if self.conn.poll(1):
                    record = self.conn.recv()
                    self.stg.insert(record)
            except EOFError as eof:
                print("eof, or pipe closed")
                break


class AggregatorNode():

    def __init__(self, host, port, master_proxy):
        super().__init__()
        self.host = host
        self.port = port
        self.master_proxy = master_proxy
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((host, port))
        self.rpc_server = SimpleXMLRPCServer(("localhost", port + 1000), allow_none=True)
        self.rpc_server.register_instance(self, allow_dotted_names=True)
        self.rpc_server_thread = threading.Thread(target=self.rpc_server.serve_forever)
        self.rpc_server_thread.daemon = True
        self.rpc_server_thread.start()
        self.procs = []
        self.pid = os.getpid()
        self.samples_count = {}
        self.rdeque = collections.deque(maxlen=100000)

    def sigint_handler(self, AggregatorProcessig, frame):
        try:
            # make sure we only do this for main process as child process also fork the __sigint_handler
            if os.getpid() == self.pid:
                print('SIGINT received, terminalting aggregator processes')
                for entry in self.procs:
                    print(f'shutting down {entry[0]}')
                    entry[0].terminate()
                print('Shutting down incoming socket...')
                self.socket.close()
                print('Shutting down RPC server...')
                self.rpc_server.shutdown()
                sys.exit(0)
        except Exception as e:
            print(e)

    def run(self):
        with self.socket as s:
            s.listen()
            # register myself as a node, send port for incoming connection
            self.feature = self.master_proxy.register_node(socket.gethostbyname("localhost"), self.port)

            # Spin up the processes, setup pipes, inform features
            cpucnt = multiprocessing.cpu_count()
            print(f"Discovered {cpucnt} cores on local system")
            for i in range(cpucnt):
                # create a new pipe
                parent_conn, child_conn = multiprocessing.Pipe()
                query_queue = multiprocessing.Queue()

                # a new instance of aggregator process
                agg_proc_obj = AggregatorProcess(child_conn, query_queue, self.feature)
                print(f'starting aggregator_process: {agg_proc_obj}')

                # start a process running ap's run for insertion
                # store in self.procs. Format: [(process, aggregator_process instance, parent_conn)]
                p = multiprocessing.Process(target=agg_proc_obj.run, args=tuple())
                p.start()
                self.procs.append((p, agg_proc_obj, parent_conn, query_queue))

            print(f"AggregatorNode listening on {self.host}:{self.port}")
            client_socket, address = s.accept()
            print(f"received connection from {address}")
            try:
                with client_socket as sock:
                    while True:
                        size_message, address = sock.recvfrom(4)
                        size = struct.unpack('!I', size_message)[0]
                        data_message, address = sock.recvfrom(size)

                        record = json.loads(data_message)
                        self.rdeque.appendleft(datetime.datetime.now())
                        # randomly pick a running process and get its connection
                        proc = random.choice(self.procs)

                        if proc[0].name not in self.samples_count:
                            self.samples_count[proc[0].name] = 0

                        self.samples_count[proc[0].name] += 1

                        # print(f'sending {record} to {proc[0]}')
                        proc[2].send(record)

            except Exception as exception:
                print(f"Exception: {exception}")
            finally:
                print(f"Stream at host {address} on thread {threading.current_thread()} "
                      f"has closed")

    def retrieve(self, stc):
        try:
            print(f'dict stc: {stc}')
            stc = STC.from_dict(stc)
            print(f'stc: {stc}')

            # first put all queries in
            for entry in self.procs:
                q = entry[3]
                q.put(stc)

            s = Statistics()
            c = collections.Counter({})


            # then retrieve results
            for entry in self.procs:
                temps, tempc = entry[2].recv()
                if temps is not None:
                    s += temps
                if tempc is not None:
                    c += tempc
            print(len(s))
            return pickle.dumps((s, c)).hex()
        except Exception:
            traceback.print_exc()

    def rr(self):
        return sum(self.samples_count.values())

    def qsize(self):
        return

    def rrpm(self):
        one_min_ago = datetime.datetime.now() - datetime.timedelta(minutes=1)
        total = 0
        for t in self.rdeque:
            if t > one_min_ago:
                total += 1
            else:
                break
        return total

    def __retrieve_from_process(stc, entry):
        # Put the stc in queue, wait to receive from parent socket
        print("CHECK")
        q = entry[3]
        q.put(stc)
        (s, c) = entry[2].recv()
        return s, c

@click.version_option(0.1)
@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.pass_context
def cli(ctx):
    """ Running the aggregator node """
    pass

@cli.command(short_help='start the aggregator node')
@click.argument('host', type=str)
@click.argument('local_port', type=str)
def start(host, local_port):
    # setup RPC to aggregator server
    master_proxy = xmlrpc.client.ServerProxy(f'http://{host}:2222/', transport=RequestsTransport(), allow_none=True)
    agg_node = AggregatorNode('localhost', int(local_port), master_proxy)
    signal.signal(signal.SIGINT, agg_node.sigint_handler)
    agg_node.run()

if __name__ == '__main__':
    cli()
