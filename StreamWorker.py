import threading
import struct
import logging
import queue
import json
import random

from Summarizer import Summarizer


class StreamWorker(threading.Thread):

    def __init__(self, client_socket, address, index, agg_server):
        super().__init__()
        self.client_socket = client_socket
        self.address = address
        self.index = index
        self.agg_server = agg_server

    def run(self) -> None:
        try:
            with self.client_socket as sock:
                while True:
                    # TODO: consider adding exit conditions
                    # TODO: consider wrapping function contents with try except statement
                    self.index['records_observed'] += 1
                    size_message, address = sock.recvfrom(4)
                    size = struct.unpack('!I', size_message)[0]
                    data_message, address = sock.recvfrom(size)

                    m = json.loads(data_message)

                    for feature in self.agg_server.feature_list:
                        if feature in m and m[feature] != 'NULL':
                            if feature not in self.agg_server.nodes_assignment:
                                pass
                                # print(f"!!! Does not support feature: {feature}")
                            elif len(self.agg_server.nodes_assignment[feature]) == 0:
                                pass
                                # print(f"!!! No nodes are handling {feature}")
                            else:
                                node = random.choice(self.agg_server.nodes_assignment[feature])
                                # print(f"SW sending records to {node[0]}")
                                node[2].put((size_message, data_message))

        except Exception as exception:
            print(f"Exception: {exception}")
        finally:
            print(f"Stream at host {self.address[0]}:{self.address[1]} on thread {threading.current_thread()} "
                  f"has closed")
