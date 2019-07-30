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

config = configparser.ConfigParser()
config.read('config.ini')
default = config['DEFAULT']

# FIXME: Generalize to remove hardcoded sections
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_addr = (default['server_address'], int(default['server_port']))
client_socket.connect(server_addr)
# TODO: Currently not using thread pool since there is only one socket and "unpack requires a buffer of 4 byte" which
#       requires locking (and a serial stream)
# thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=5)


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
            print(f'Submitted {path} to pool')
            # TODO: Note that running this on the same machine can lead to out of memory errors
            #       from too many open files.
            file = sftp_client.open(path)
            emit_results(file)
            # thread_pool.submit(emit_results, file)
    except IOError as e:
        print(e)


# Note that the txt files are not formatted as traditional csv files. It is space delimited.
def emit_results(file):
    file.prefetch()
    for line in file:
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
        print(f'Sent {s}')
    file.close()


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
    local_addr = (socket.gethostbyname(socket.gethostname()), port)
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

    sftp_client = jhost.open_sftp()

    remote_list_files(data_dir, sftp_client, default.get('recursive', False))

    # thread_pool.shutdown()
    jhost.close()
    ssh_client.close()
    client_socket.close()
