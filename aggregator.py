import pandas as pd
import socket
import messages_pb2 as pbs

# create an INET, STREAMing socket
serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# bind the socket to a public host, and a well-known port
serversocket.bind(('localhost', 55555))
# become a server socket
serversocket.listen(5)

while True:
    # accept connections from outside
    (clientsocket, address) = serversocket.accept()
    print(address)
    # now do something with the clientsocket
    # in this case, we'll pretend this is a threaded server
    # ct = client_thread(clientsocket)
    # ct.run()
    record = pbs.Record()
    record.ParseFromString(clientsocket.recv(4096))
    print(record.PRECIPITATION)
