import threading, struct, logging, queue, json

from Summarizer import Summarizer

class StreamWorker(threading.Thread):

    def __init__(self, client_socket, address, index, queueList):
        super().__init__()
        self.client_socket = client_socket
        self.address = address
        self.index = index
        self.queueList = queueList

    def run(self) -> None:

        try:
            with self.client_socket as sock:
                while True:
                    # todo : consider adding exit conditions
                    # todo : consider wrapping function contents with try except statement
                    self.index['records_observed'] += 1
                    size_message, address = sock.recvfrom(4)
                    size = struct.unpack('!I', size_message)[0]
                    data_message, address = sock.recvfrom(size)

                    self.queueList.put(json.loads(data_message))
                    # print(json.loads(data_message))
                    # logging.log(f"size: {size}, msg: {data_message}")
                    # print(f"size: {size}, msg: {data_message}")
        except Exception as exception:
            print(f"Exception: {exception}")
        finally:
            print(f"Stream at host {self.address[0]}:{self.address[1]} on thread {threading.current_thread()} has closed")
