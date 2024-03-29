import configparser
import paramiko
import socket
import concurrent.futures
import getpass
import sys
import os
from stat import S_ISDIR
import json
import struct
import multiprocessing
import time
import signal

config = configparser.ConfigParser()
config.read('config.ini')
default = config['DEFAULT']

# FIXME: Generalize to remove hardcoded sections
# TODO: Currently not using thread pool since there is only one socket and "unpack requires a buffer of 4 byte" which
#       requires locking (and a serial stream)
# thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=5)
task_queue = multiprocessing.Queue()

procs = []

mainpid = os.getpid()

def put_dir(sftp_client, source, target):
    ''' Uploads the contents of the source directory to the target path. The
        target directory needs to exists. All subdirectories in source are
        created under target.
    '''
    for item in os.listdir(source):
        print(item)
        if os.path.isfile(os.path.join(source, item)):
            sftp_client.put(os.path.join(source, item), '%s/%s' % (target, item))
        else:
            mkdir(sftp_client, '%s/%s' % (target, item), ignore_existing=True)
            put_dir(sftp_client, os.path.join(source, item), '%s/%s' % (target, item))

def mkdir(sftp_client, path, mode=511, ignore_existing=False):
    ''' Augments mkdir by adding an option to not fail if the folder exists  '''
    try:
        sftp_client.mkdir(path, mode)
    except IOError:
        if ignore_existing:
            pass
        else:
            raise



def remote_list_files(path: str, sftp: paramiko.SSHClient, recursive: bool) -> None:
    try:
        if S_ISDIR(sftp_client.stat(path).st_mode):
            file_list = sftp.listdir(path=path)
            path = path.rstrip(os.sep)
            if recursive:
                for item in file_list:
                    remote_list_files(path + os.sep + item, sftp, recursive)
            else:
                print('Unable to emit directory. Did you want recursion?')
        else:
            # print(f'Submitted {path} to pool')
            # TODO: Note that running this on the same machine can lead to out of memory errors
            #       from too many open files.
            file = sftp_client.open(path)
            file.prefetch()
            task_queue.put(file.readlines())
            # thread_pool.submit(emit_results, file)
    except IOError as e:
        print(e)


def emitting_worker():
    print(f"Starting emitter process {os.getpid()} ")
    while True:
        if not task_queue.empty():
            lines = task_queue.get()
            emit_results(lines)
        else:
            time.sleep(0.2)

# Note that the txt files are not formatted as traditional csv files. It is space delimited.
def emit_results(lines):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_addr = (default['server_address'], int(default['server_port']))
    client_socket.connect(server_addr)
    for line in lines:
        parts = line.split()
        try:
            s = {'UTC_DATE': int(parts[1]),
                 'UTC_TIME': int(parts[2]),
                 'LONGITUDE': float(parts[6]),
                 'LATITUDE': float(parts[7]),
                 'AIR_TEMPERATURE': float(parts[8]),
                 'PRECIPITATION': float(parts[9]),
                 'SOLAR_RADIATION': int(parts[10]),
                 'SURFACE_TEMPERATURE': float(parts[12]),
                 'RELATIVE_HUMIDITY': int(parts[15]),
                 }
        except ValueError:
            print(f'Could not parse: {parts}. Continuing...')
            continue
        serialized = str.encode(json.dumps(s))
        len_in_binary = struct.pack('!I', len(serialized))
        # send length of next message in four bytes exactly
        client_socket.sendto(len_in_binary, server_addr)
        # send actual message
        client_socket.sendto(serialized, server_addr)
        # print(f'Sent {s}')
    file.close()

def sigint_handler(_1, _2):
    try:
     # make sure we only do this for main process as child process also fork the __sigint_handler
        if os.getpid() == mainpid:
            print('SIGINT received, terminalting aggregator processes')
            for entry in procs:
                print(f'shutting down {entry}')
                entry.terminate()
            sys.exit(0)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    # stargate in this case
    hostname = default['hostname']
    # orion01 in this case
    jumphostIP = default['jump_host']
    data_dir = default['path']
    port = int(default['port'])

    if 'username' in default:
        user = default['username']
    else:
        user = input("Username: ")
    key_login = False

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        # Attempt to use SSH key
        ssh_client.connect(hostname=hostname, username=user, port=port)
        print('Successful login using ssh key!')
        key_login = True
    except paramiko.ssh_exception.AuthenticationException as error:
        # If unable to use SSH key, ask for password
        try:
            if 'password' in default:
                p = default['password']
            else:
                p = getpass.getpass()
            ssh_client.connect(hostname=hostname, username=user, password=p, port=port)
            print('Successful login!')
        except paramiko.ssh_exception.AuthenticationException as error:
            print('Error: ', error)
            ssh_client.close()
            sys.exit()
        except Exception as error:
            print('Error: ', error)
            ssh_client.close()
            sys.exit()
    except Exception as error:
        print('Error: ', error)
        ssh_client.close()
        sys.exit()

    transport = ssh_client.get_transport()
    dest_addr = (jumphostIP, port)
    local_addr = (socket.gethostbyname('localhost'), port)
    channel = transport.open_channel("direct-tcpip", dest_addr, local_addr)
    #
    jhost = paramiko.SSHClient()
    jhost.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    if key_login:
        try:
            # Attempt to use SSH key
            jhost.connect(jumphostIP, username=user, sock=channel, port=port)
            print('Successful login using ssh key! (jumphost)')
        except paramiko.ssh_exception.AuthenticationException as error:
            # If unable to use SSH key, ask for password
            try:
                jhost.connect(jumphostIP, username=user, password=p, sock=channel, port=port)
                print('Successful login!')
            except paramiko.ssh_exception.AuthenticationException as error:
                print('Error: ', error)
                ssh_client.close()
                jhost.close()
                sys.exit()
            except Exception as error:
                print('Error: ', error)
                ssh_client.close()
                jhost.close()
                sys.exit()
        except Exception as error:
            print('Error: ', error)
            ssh_client.close()
            jhost.close()
            sys.exit()
    else:
        try:
            jhost.connect(jumphostIP, username=user, sock=channel, port=port)
            print('Successful login!')
        except paramiko.ssh_exception.AuthenticationException as error:
            print('Error: ', error)
            ssh_client.close()
            jhost.close()
            sys.exit()
        except Exception as error:
            print('Error: ', error)
            ssh_client.close()
            jhost.close()
            sys.exit()

    global sftp_client
    sftp_client = jhost.open_sftp()





    # pool_size = int(default['pool_size'])
    # for i in range(pool_size):
    #     p = multiprocessing.Process(target=emitting_worker, args=())
    #     p.start()
    #     procs.append(p)
    #
    # signal.signal(signal.SIGINT, sigint_handler)
    #
    # remote_list_files(data_dir, sftp_client, default.get('recursive', False))
    #
    # while not task_queue.empty():
    #     time.sleep(1)

    # thread_pool.shutdown()
    jhost.close()
    ssh_client.close()
    client_socket.close()
