import click
import configparser
import paramiko
import socket







@click.version_option(0.1)
@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.pass_context
def cli(ctx):
    """ Running the aggregator node """
    pass

@cli.command(short_help='start the emitter cluster')
@click.argument('clustercfg', type=str)
def start(clustercfg):
    # setup RPC to aggregator server

	config = configparser.ConfigParser()
	config.read(clustercfg)

	if config.has_section('emitters'):
		print(config.items("emitters"))
		for emitter in config.items("emitters"):
			host = emitter[0]
			path = emitter[1]

			command = f"python3 DataEmitter.py {config.get('settings','mode')} {path} {config.get('settings', 'master_host')} "
			if config.get('settings','mode') == 'serial':
				if config.has_option('settings', 'interval'):
					command += f"-i {config.get('settings', 'interval')} "
				if config.has_option('settings', 'parallel'):
					command += "--parallel "

			command += "&"

			print(f"{host}: {command}")
			jhost = get_ssh_client(config, host)
			stdin, stdout, stderr = jhost.exec_command(command)
			print(stderr.readlines())

def get_ssh_client(clustercfg, host):
	hostname = 'stargate.cs.usfca.edu'
	if 'username' in clustercfg['settings']:
		user = clustercfg['settings']['username']
	else:
		user = input("Username: ")
	key_login = False

	port = 22
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
	dest_addr = (host, port)
	local_addr = (socket.gethostbyname('localhost'), port)
	channel = transport.open_channel("direct-tcpip", dest_addr, local_addr)
	#
	jhost = paramiko.SSHClient()
	jhost.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	if key_login:
		try:
			# Attempt to use SSH key
			jhost.connect(host, username=user, sock=channel, port=port)
			print('Successful login using ssh key! (jumphost)')
		except paramiko.ssh_exception.AuthenticationException as error:
			# If unable to use SSH key, ask for password
			try:
				jhost.connect(host, username=user, password=p, sock=channel, port=port)
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
			jhost.connect(host, username=user, sock=channel, port=port)
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

	return jhost









if __name__ == '__main__':
    cli()
