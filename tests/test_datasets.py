import json
import os
import requests
import tempfile
import time
import pytest
import redis
import requests
import shutil
import signal
import threading

from datetime import datetime, timedelta
from subprocess import Popen

from comet import Manager
from chimedb.dataset import get_state, get_dataset, DatasetState, Dataset
import chimedb.core as chimedb
from comet.manager import TIMESTAMP_FORMAT

CHIMEDBRC = os.path.join(os.getcwd() + "/.chimedb_test_rc")
CHIMEDBRC_MESSAGE = "Could not find {}.".format(CHIMEDBRC)
PORT = "8000"

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

# Todo: deprecated
@pytest.fixture(scope="session", autouse=True)
def manager():
    manager = Manager("localhost", PORT)

    manager.register_start(now, version)
    manager.register_config(CONFIG)
    return manager


@pytest.fixture(scope="session", autouse=True)
def manager_new():
    manager = Manager("localhost", PORT)

    manager.register_start(now, version, CONFIG)
    return manager


@pytest.fixture(scope="session", autouse=True)
def broker():
    # Tell chimedb where the database connection config is
    assert os.path.isfile(CHIMEDBRC), CHIMEDBRC_MESSAGE
    os.environ["CHIMEDB_TEST_RC"] = CHIMEDBRC

    # Make sure we don't write to the actual chime database
    os.environ["CHIMEDB_TEST_ENABLE"] = "Yes, please."

    broker = Popen(["comet", "--debug", "1", "-p", PORT])
    time.sleep(3)
    yield
    os.kill(broker.pid, signal.SIGINT)


@pytest.fixture(scope="session", autouse=True)
def archiver(broker):
    archiver = Popen(
        ["comet_archiver", "-t", "10", "--broker_port", PORT, "--log_level", "DEBUG"]
    )
    yield dir
    pid = archiver.pid
    os.kill(pid, signal.SIGINT)
    archiver.terminate()


@pytest.fixture(scope="session", autouse=False)
def simple_ds(manager):
    state_id = manager.register_state({"foo": "bar"}, "test")
    dset_id = manager.register_dataset(state_id, None, "test", True)
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


# TODO: register stuff here, then with a new broker test recovery in test_recover
def test_register(manager, broker):
    pass


def test_recover(manager, broker, simple_ds):
    dset_id = simple_ds[0]

    # Give archiver a moment
    time.sleep(2)
    assert manager.broker_status()
    manager.register_config({"blubb": 1})
    time.sleep(0.1)

    ds = manager.get_dataset(dset_id)
    state = manager.get_state("test")
    assert state == {"foo": "bar", "type": "test"}
    assert ds["is_root"] is True
    # TODO: fix hash function # assert ds["state"] == manager._make_hash(state)
    assert ds["type"] == "test"


def test_archiver(archiver, simple_ds, manager, broker):
    dset_id = simple_ds[0]
    state_id = simple_ds[1]

    # Tell chimedb where the database connection config is
    assert os.path.isfile(CHIMEDBRC), CHIMEDBRC_MESSAGE
    os.environ["CHIMEDB_TEST_RC"] = CHIMEDBRC

    # Make sure we don't write to the actual chime database
    os.environ["CHIMEDB_TEST_ENABLE"] = "foo"

    # Open database connection
    chimedb.connect()

    ds = get_dataset(dset_id)

    assert ds.state.id == state_id
    assert ds.root is True

    state = get_state(state_id)
    assert state.id == state_id
    assert state.type.name == "test"
    assert state.data == {"foo": "bar", "type": "test"}

    chimedb.close()


def test_archiver_pushback(archiver):
    r = redis.Redis("127.0.0.1", 6379)
    assert r.llen("archive_dataset") == 0
    assert r.llen("archive_state") == 0

    # remove from redis and DB to make this test behave the same if run twice
    Dataset.delete().where(Dataset.id == "test_ds").execute()
    DatasetState.delete().where(DatasetState.id == "test_state").execute()
    r.hdel("states", "test_state")
    r.hdel("datasets", "test_ds")

    r.lpush(
        "archive_state",
        json.dumps({"hash": "test_state", "time": "1999-01-01-10:10:42.001"}),
    )
    time.sleep(0.1)

    # we are testing the ength of the archiver's input list. It's either 1, or 0 (if the
    # is looking at the entry right now.
    llen = r.llen("archive_state")
    assert llen == 1 or llen == 0

    r.lpush(
        "archive_dataset",
        json.dumps({"hash": "test_ds", "time": "1999-01-01-10:10:42.001"}),
    )
    time.sleep(0.1)
    llen = r.llen("archive_dataset")
    assert llen == 1 or llen == 0

    r.hset("datasets", "test_ds", json.dumps({"is_root": True, "state": "test_state"}))
    time.sleep(0.1)
    r.llen("archive_dataset")
    assert llen == 1 or llen == 0
    r.llen("archive_state")
    assert llen == 1 or llen == 0

    r.lpush(
        "archive_state",
        json.dumps({"hash": "test_state", "time": "1999-01-01-10:10:42.001"}),
    )
    r.lpush(
        "archive_dataset",
        json.dumps({"hash": "test_ds", "time": "1999-01-01-10:10:42.001"}),
    )
    time.sleep(0.1)
    llen = r.llen("archive_state")
    assert llen == 1 or llen == 2
    llen = r.llen("archive_dataset")
    assert llen == 1 or llen == 2

    r.hset(
        "states", "test_state", json.dumps({"state": "test_state", "type": "bs_state"})
    )
    time.sleep(0.1)
    assert r.llen("archive_dataset") == 0
    assert r.llen("archive_state") == 0


def test_status(simple_ds, manager):
    assert simple_ds[0] in manager._get_datasets()
    assert simple_ds[1] in manager._get_states()


def test_gather_update(simple_ds, manager, broker):
    root = simple_ds[0]
    state_id = manager.register_state({"f00": "b4r"}, "t3st")
    dset_id0 = manager.register_dataset(state_id, root, "test", False)
    state_id = manager.register_state({"f00": "br"}, "t3st")
    dset_id1 = manager.register_dataset(state_id, dset_id0, "test", False)
    state_id = manager.register_state({"f00": "b4"}, "t3st")
    dset_id2 = manager.register_dataset(state_id, dset_id1, "test", False)

    result = requests.post(
        "http://localhost:{}/update-datasets".format(PORT),
        json={"ds_id": dset_id2, "ts": 0, "roots": [root]},
    ).json()
    assert "datasets" in result
    assert dset_id0 in result["datasets"]
    assert dset_id1 in result["datasets"]
    assert dset_id2 in result["datasets"]
