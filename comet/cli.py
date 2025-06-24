"""CLI interface to run comet."""

import logging

import click

from .archiver import Archiver
from .broker import Broker

# LOG_FORMAT = "[%(asctime)s] %(name)s: %(message)s"
logging.basicConfig(format="[%(asctime)s] %(name)s: %(message)s")


# ===============
# Broker defaults
# ===============
DEFAULT_BROKER_PORT = 12050

# =================
# Archiver defaults
# =================
DEFAULT_FAILURE_WAIT_TIME = 500  # milliseconds
DEFAULT_BROKER_HOST = "127.0.0.1"
DEFAULT_REDIS_HOST = "127.0.0.1"
DEFAULT_REDIS_PORT = 6379


@click.group()
def cli():
    """Run Comet."""


@cli.command()
@click.option(
    "-p",
    "--port",
    help="Set the port.",
    type=int,
    default=DEFAULT_BROKER_PORT,
)
@click.option(
    "-t",
    "--file-lock-time",
    help="DEPRECATED. Set file lock time in seconds.",
    type=int,
    default=0,
)
@click.option(
    "--timeout",
    help="Set timeout in seconds.",
    type=int,
    default=40,
)
@click.option(
    "--debug",
    is_flag=True,
    help="Run in debug mode",
)
@click.option(
    "--recover",
    is_flag=True,
    help="DEPRECATED. Set recover mode. If you really need to do this, use `redis-cli flushall`.",
)
def broker(port, file_lock_time, timeout, debug, recover):
    """Run the comet broker."""
    broker = Broker(debug, port, timeout)
    broker.run_comet()


@cli.command()
@click.option(
    "-t",
    "--failure-wait-time",
    help="Time to wait (in milliseconds) if data can't immediately be archived.",
    type=int,
    default=DEFAULT_FAILURE_WAIT_TIME,
)
@click.option(
    "--broker-host",
    help="Set the host on which the dataset broker is running.",
    type=str,
    default=DEFAULT_BROKER_HOST,
)
@click.option(
    "--broker-port",
    help="Set the port on which the dataset brokeris running.",
    type=int,
    default=DEFAULT_BROKER_PORT,
)
@click.option(
    "--redis-host",
    help="Set the redis host.",
    type=str,
    default=DEFAULT_REDIS_HOST,
)
@click.option(
    "--redis-port",
    help="Set the redis port.",
    type=int,
    default=DEFAULT_REDIS_PORT,
)
@click.option(
    "--log-level",
    help="Set the logging level.",
    type=str,
    default="INFO",
)
def archiver(
    failure_wait_time, broker_host, broker_port, redis_host, redis_port, log_level
):
    """Run the comet archiver."""
    archiver = Archiver(
        broker_host, broker_port, redis_host, redis_port, log_level, failure_wait_time
    )
    archiver.run()
