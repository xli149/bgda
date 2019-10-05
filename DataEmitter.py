import json
import struct
import time
import pandas as pd
import glob
import threading
import click
import sys
import os
import optparse
import abc
import socket
import datetime
import signal
import multiprocessing

class EmitterBase(abc.ABC):
    def __init__(self, host, port):
        self.server_addr = (host, port)

    @abc.abstractmethod
    def start(self):
        pass

    def get_sock(self):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(self.server_addr)
        return client_socket


class DataEmitter(EmitterBase):
    def __init__(self, host, port, files):
        super(DataEmitter, self).__init__(host, port)
        self.files = files
        self.shutdown = False
        print(f"emitting {self.files}")
        self.columns = ['WBANNO', 'UTC_DATE', 'UTC_TIME', 'LST_DATE', 'LST_TIME', 'CRX_VN', 'LONGITUDE',
                             'LATITUDE', 'AIR_TEMPERATURE', 'PRECIPITATION', 'SOLAR_RADIATION', 'SR_FLAG',
                             'SURFACE_TEMPERATURE', 'ST_TYPE', 'ST_FLAG',
                             'RELATIVE_HUMIDITY', 'RH_FLAG', 'SOIL_MOISTURE_5', 'SOIL_TEMPERATURE_5', 'WETNESS',
                             'WET_FLAG', 'WIND_1_5', 'WIND_FLAG']
        signal.signal(signal.SIGINT, self.sigint_handler)
        self.msg_queue = multiprocessing.Queue()
        self.sender_thread = threading.Thread(target=self.sender_thread, args=())
        self.sender_thread.daemon = True
        self.sender_thread.start()
        self.pid = os.getpid()

    def sigint_handler(self, sig, frame):
        self.shutdown = True
        try:
            # make sure we only do this for main process as child process also fork the __sigint_handler
            if os.getpid() == self.pid:
                print('SIGINT received')
                if hasattr(self, 'procs'):
                    print('terminalting emitter processes')
                    for proc in self.procs:
                        print(f'shutting down {proc}')
                        proc.terminate()
                print('Shutting down outgoing socket...')
                self.sender_thread.join()
                print("done")
                os._exit(0)
        except Exception as e:
            print(e)

    def sender_thread(self):
        self.sock = self.wait_for_connection()
        while not self.shutdown:
            while not self.msg_queue.empty():
                # print('ch')
                s = self.msg_queue.get()
                print(f"emits: {s}")
                serialized = str.encode(json.dumps(s))
                len_in_binary = struct.pack('!I', len(serialized))
                try:
                    if self.shutdown:
                        break
                    # print(self.shutdown)
                    self.sock.sendto(len_in_binary, self.server_addr)
                    self.sock.sendto(serialized, self.server_addr)
                except BrokenPipeError:
                    self.sock = self.wait_for_connection()
            time.sleep(0.5)
        if self.sock:
            self.sock.close()


    def wait_for_connection(self):
        self.connected = False
        sock = None
        while not self.connected and not self.shutdown:
            try:
                sock = self.get_sock()
                self.connected = True
            except Exception:
                print("waiting for AggregatorServer...")
                time.sleep(3)
                continue
        return sock



class RealTimeEmitter(DataEmitter):
    '''
        Emitter that can emit records in real-time.

        For example:
            If now is 2019-9-1 17:00:00 and RealTimeEmitter is emitting 2018 file.
            RealTimeEmitter will ignore year difference, and skip jan-august and
            start emitting from 2018-9-1 17:00:00 and emit according to the time
            of the records in the file

        RealTimeEmitter is the best implementation to simulate realistic emissions,
        because it simulates the actual data density and date

        Note: If RealTimeEmitter is assigned with a list of files, it will create
        one thread for each file and follow the above logic for each file
    '''


    def start(self):
        self.procs = []
        for file in self.files:
            p = multiprocessing.Process(target=self.emit_file, args=(file, self.msg_queue))
            self.procs.append(p)
            p.start()

    def emit_file(self, file, queue):
        '''
            Single file emission logic:

            1. For each line in file, interpret date from record (replace current year for comparison)
            2. If date is in past, skip
            3. If date is now, emit
            4. If date is in future, wait until it is passed now

        '''
        dfmt = '%Y%m%d'
        data = pd.read_csv(file, header=None, delimiter=r'\s+')
        data.columns = self.columns
        now = datetime.datetime.now()
        print("searching for next emission...")
        for index, record in data.iterrows():
            t = record.UTC_TIME
            dt = datetime.datetime.strptime(str(record.UTC_DATE), dfmt)
            dt = dt.replace(hour=t//100, minute=t%100, year = now.year)
            if dt < datetime.datetime.now():
                continue

            s = {'UTC_DATE': record.UTC_DATE,
                 'UTC_TIME': record.UTC_TIME,
                 'LONGITUDE': record.LONGITUDE,
                 'LATITUDE': record.LATITUDE,
                 'AIR_TEMPERATURE': record.AIR_TEMPERATURE,
                 'PRECIPITATION': record.PRECIPITATION,
                 'SOLAR_RADIATION': record.SOLAR_RADIATION,
                 'SURFACE_TEMPERATURE': record.SURFACE_TEMPERATURE,
                 'RELATIVE_HUMIDITY': record.RELATIVE_HUMIDITY,
                 }
            if dt == datetime.datetime.now():
                queue.put(s)
            else:
                print(f"next emission: {s['UTC_DATE']}:{s['UTC_TIME']}")
                while dt > datetime.datetime.now():
                    time.sleep(1)
                queue.put(s)

class SerialEmitter(DataEmitter):
    def start(self, interval=1, parallel=False):
        if parallel:
            print("Serial: parallel mode")
            self.procs = []
            for file in self.files:
                p = multiprocessing.Process(target=self.emit_file, args=(file, self.msg_queue, interval,))
                self.procs.append(p)
                p.start()
        else:
            print("Serial: Single thread mode")
            for file in self.files:
                self.emit_file(file, self.msg_queue, interval)


    def emit_file(self, file, queue, interval):
        dfmt = '%Y%m%d'
        data = pd.read_csv(file, header=None, delimiter=r'\s+')
        data.columns = self.columns
        for index, record in data.iterrows():
            s = {'UTC_DATE': record.UTC_DATE,
                 'UTC_TIME': record.UTC_TIME,
                 'LONGITUDE': record.LONGITUDE,
                 'LATITUDE': record.LATITUDE,
                 'AIR_TEMPERATURE': record.AIR_TEMPERATURE,
                 'PRECIPITATION': record.PRECIPITATION,
                 'SOLAR_RADIATION': record.SOLAR_RADIATION,
                 'SURFACE_TEMPERATURE': record.SURFACE_TEMPERATURE,
                 'RELATIVE_HUMIDITY': record.RELATIVE_HUMIDITY,
                 }
            queue.put(s)
            time.sleep(interval)


@click.version_option(0.1)
@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.pass_context
def cli(ctx):
    """ Running the data emitter """
    pass

@cli.command(short_help='emit file in realtime mode')
@click.argument('path', type=click.Path(exists=True))
@click.argument('host', type=str)
def rt(path, host):
    port = int(host.split(':')[1])
    host = host.split(':')[0]
    files = []
    if os.path.isdir(path):
        files = glob.glob(f"{path}/**/CRN*.txt", recursive=True)
    else:
        files = [path]
    RealTimeEmitter(host, port, files).start()


@cli.command(short_help='emit file in serial mode')
@click.argument('path', type=click.Path(exists=True))
@click.argument('host', type=str)
@click.option('--interval', '-i', type=int, default=1)
@click.option('--parallel', is_flag=True, default=False)
def serial(path, host, interval, parallel):
    port = int(host.split(':')[1])
    host = host.split(':')[0]
    files = []
    if os.path.isdir(path):
        files = glob.glob(f"{path}/**/CRN*.txt", recursive=True)
    else:
        files = [path]
    SerialEmitter(host, port, files).start(interval, parallel)

if __name__ == '__main__':
    cli()
    # if len(sys.argv != 1):
    #     print("usage: DataEmitter.py <file>")
    #
    # txt_file = sys.argv[0]
    # txt_file = "2018/CRNS0101-05-2018-KS_Manhattan_6_SSW.txt"
    # DataEmitter('localhost', 55555, txt_file).start()

    # txt_files = glob.glob("2006/CRN*.txt")
    #
    # print("num of files: " + str(len(txt_files)))
    #
    # emitters = []
    #
    # for file in txt_files:
    #     print(f"Reading file: {file}")
    #     emitters.append(DataEmitter('localhost', 55554, file))
    #
    # threads = []
    # for emitter in emitters:
    #     t = threading.Thread(target=emitter.start)
    #     threads.append(t)
    #     t.start()
    #     print(f"starting thread {t}")
