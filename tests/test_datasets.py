import json
import os
import tempfile
import time
import pytest
import shutil
import signal

from subprocess import Popen

from datetime import datetime, timedelta
from comet import Manager
from comet.broker import DEFAULT_PORT
from chimedb.dataset import get_state, get_dataset, get_types
import chimedb.core as chimedb
from comet.manager import TIMESTAMP_FORMAT

CHIMEDBRC = os.path.join(os.getcwd() + "/.chimedbrc")
CHIMEDBRC_MESSAGE = (
    "Could not find {}. It is important that this test uses this "
    "file to connect to a dummy database. Otherwise it could write "
    "into a production database.".format(CHIMEDBRC)
)

# Some dummy states for testing:
CONFIG = {"a": 1, "b": "fubar"}
ABC = {"a": 0, "b": 1, "c": 2, "d": 3}
A = {"a": 1, "b": "fubar"}
B = {"a": 1, "b": "fubar"}
C = {"a": 1, "b": "fuba"}
D = {"a": 1, "c": "fubar"}
E = {"a": 2, "b": "fubar"}
F = {"a": 1}
G = {"b": 1}
H = {"blubb": "bla"}
J = {"meta": "data"}

now = datetime.utcnow()
version = "0.1.1"
dir = tempfile.mkdtemp()


@pytest.fixture(scope="session", autouse=True)
def manager():
    manager = Manager("localhost", DEFAULT_PORT)

    # Wait for broker to start up.
    time.sleep(0.1)

    manager.register_start(now, version)
    manager.register_config(CONFIG)
    return manager


@pytest.fixture(scope="session", autouse=True)
def broker():
    # Make sure we don't write to the actual chime database
    assert os.path.isfile(CHIMEDBRC), CHIMEDBRC_MESSAGE
    os.environ["CHIMEDBRC"] = CHIMEDBRC

    broker = Popen(["comet", "--debug", "1", "-d", dir, "-t", "2"])
    time.sleep(3)
    yield dir
    pid = broker.pid
    os.kill(pid, signal.SIGINT)
    broker.terminate()

    # Give the broker a moment to delete the .lock file
    time.sleep(0.1)
    shutil.rmtree(dir)


@pytest.fixture(scope="function", autouse=True)
def archiver():
    archiver = Popen(["comet_archiver", "-d", dir, "-i", "1"])
    yield dir
    pid = archiver.pid
    os.kill(pid, signal.SIGINT)
    archiver.terminate()


@pytest.fixture(scope="session", autouse=False)
def simple_ds(manager):
    state_id = manager.register_state({"foo": "bar"}, "test")
    dset_id = manager.register_dataset(state_id, None, ["test"], True)

    yield (dset_id, state_id)


def test_hash(manager):
    assert isinstance(manager, Manager)

    assert manager._make_hash(A) == manager._make_hash(A)
    assert manager._make_hash(A) == manager._make_hash(B)
    assert manager._make_hash(A) != manager._make_hash(C)
    assert manager._make_hash(A) != manager._make_hash(D)
    assert manager._make_hash(A) != manager._make_hash(E)


def test_register_config(manager, broker):

    expected_config_dump = CONFIG
    expected_config_dump["type"] = "config_{}".format(__name__)

    assert expected_config_dump == manager.get_state()

    # Find the dump file
    dump_files = os.listdir(broker)
    dump_files = list(filter(lambda x: x.endswith("data.dump"), dump_files))
    dump_times = [f[:-10] for f in dump_files]
    dump_times = [datetime.strptime(t, TIMESTAMP_FORMAT) for t in dump_times]
    freshest = dump_times.index(max(dump_times))

    with open(os.path.join(broker, dump_files[freshest]), "r") as json_file:
        comet_start_dump = json.loads(json_file.readline())
        comet_config_dump = json.loads(json_file.readline())

        start_dump = json.loads(json_file.readline())
        config_dump = json.loads(json_file.readline())

    assert comet_start_dump["state"]["type"] == "start_comet.broker"
    assert comet_config_dump["state"]["type"] == "config_comet.broker"

    expected_start_dump = {
        "time": now.strftime(TIMESTAMP_FORMAT),
        "version": version,
        "type": "start_{}".format(__name__),
    }
    assert start_dump["state"] == expected_start_dump
    assert start_dump["hash"] == manager._make_hash(expected_start_dump)
    assert datetime.strptime(
        start_dump["time"], TIMESTAMP_FORMAT
    ) - datetime.utcnow() < timedelta(minutes=1)
    assert datetime.strptime(
        start_dump["state"]["time"], TIMESTAMP_FORMAT
    ) - datetime.utcnow() < timedelta(minutes=1)

    assert config_dump["state"] == expected_config_dump
    assert datetime.strptime(
        config_dump["time"], TIMESTAMP_FORMAT
    ) - datetime.utcnow() < timedelta(minutes=1)
    assert config_dump["hash"] == manager._make_hash(expected_config_dump)


# TODO: register stuff here, then with a new broker test recovery in test_recover
def test_register(manager, broker):
    pass


def test_recover(manager, broker, simple_ds):
    dset_id = simple_ds[0]

    # Give archiver a moment and make broker release dump file by registering another state.
    time.sleep(2)
    manager.register_config({"blubb": 1})
    time.sleep(0.1)

    ds = manager.get_dataset(dset_id)
    state = manager.get_state("test")
    assert state == {"foo": "bar", "type": "test"}
    assert ds["is_root"] is True
    # TODO: fix hash function # assert ds["state"] == manager._make_hash(state)
    assert ds["types"] == ["test"]


def test_archiver(archiver, simple_ds, manager):
    dset_id = simple_ds[0]
    state_id = simple_ds[1]

    # Make sure we don't write to the actual chime database
    assert os.path.isfile(CHIMEDBRC), CHIMEDBRC_MESSAGE
    os.environ["CHIMEDBRC"] = CHIMEDBRC

    # Open database connection
    chimedb.connect()

    ds = get_dataset(dset_id)
    assert ds.state.id == state_id
    assert ds.root is True

    types = get_types(dset_id)
    assert types == ["test"]

    state = get_state(state_id)
    assert state.id == state_id
    assert state.type.name == types[0]
    assert state.data == {"foo": "bar", "type": "test"}

    chimedb.close()
