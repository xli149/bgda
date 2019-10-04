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
@click.argument('clustercfg', type=click.File('r'))
def start(clustercfg):
    # setup RPC to aggregator server

	config = configparser.ConfigParser()
	config.read(clustercfg)

	command = "python3 DataEmitter.py emit"
	if config.has_section('emitters'):
		for emitter in config.items("emitters"):
			host = emitter[0]
			path = emitter[1]







if __name__ == '__main__':
    cli()
