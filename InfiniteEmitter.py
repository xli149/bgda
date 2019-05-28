from EmitterBase import EmitterBase
import json
import math
import time
import struct


class InfiniteEmitter(EmitterBase):

    def __init__(self, host, port):
        super(InfiniteEmitter, self).__init__(host, port)

    def start(self):
        with self.client_socket as sock:
            for data in self.data_generator():

                serialized = str.encode(json.dumps(data))

                len_in_binary = struct.pack('!I', len(serialized))

                # send length of next message in four bytes exactly
                sock.sendto(len_in_binary, self.server_addr)

                # send actual message
                sock.sendto(serialized, self.server_addr)

                # time.sleep(0.5)

    @staticmethod
    def data_generator():
        ix = 1
        while True:
            yield {'a': 0,
                   'b': 3 * ix + 10,
                   'c': -1 * ix,
                   'd': 0.01 * ix,
                   'e': ix**2,
                   'f': math.sin(ix),
                   'g': math.log(ix)}
            ix += 1


if __name__ == '__main__':
    emitter = InfiniteEmitter('localhost', 5556)
    emitter.start()
