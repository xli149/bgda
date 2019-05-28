import abc
import socket


class EmitterBase(abc.ABC):
    def __init__(self, host, port):
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_addr = (host, port)
        self.client_socket.connect(self.server_addr)

    @abc.abstractmethod
    def start(self):
        pass
