import json
import os
import time
import pytest
import redis
import requests
import signal

from datetime import datetime
from subprocess import Popen

from comet import Manager, BrokerError, State, Dataset
from comet.hash import hash_dictionary
from comet.manager import REGISTER_DATASET

import chimedb.dataset
import chimedb.core

CHIMEDBRC = os.path.join(os.getcwd() + "/.chimedb_test_rc")
CHIMEDBRC_MESSAGE = "Could not find {}.".format(CHIMEDBRC)
PORT = "8000"
PORT_LOW_TIMEOUT = "8080"

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


try:
    chimedb.core.connect()
    chimedb.core.close()
    has_chimedb = True
except chimedb.core.exceptions.ConnectionError:
    has_chimedb = False


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

    assert manager.register_start(now, version, CONFIG) is None
    return manager


@pytest.fixture(scope="session", autouse=True)
def manager_and_dataset():
    manager = Manager("localhost", PORT)

    ds = manager.register_start(now, version, CONFIG, register_datasets=True)
    return manager, ds


@pytest.fixture(scope="session", autouse=True)
def manager_low_timeout():
    """Start manager that uses low-timeout broker."""
    manager = Manager("localhost", PORT_LOW_TIMEOUT)

    manager.register_start(now, version, CONFIG)
    return manager


@pytest.fixture(scope="session", autouse=True)
def broker_low_timeout():
    """Start a broker with timeout of 1s."""
    # Tell chimedb where the database connection config is
    assert os.path.isfile(CHIMEDBRC), CHIMEDBRC_MESSAGE
    os.environ["CHIMEDB_TEST_RC"] = CHIMEDBRC

    # Make sure we don't write to the actual chime database
    os.environ["CHIMEDB_TEST_ENABLE"] = "Yes, please."

    broker = Popen(["comet", "--debug", "1", "-p", PORT_LOW_TIMEOUT, "--timeout", "1"])

    # wait for broker start
    time.sleep(3)
    yield
    os.kill(broker.pid, signal.SIGINT)


@pytest.fixture(scope="session", autouse=True)
def broker():
    # Tell chimedb where the database connection config is
    assert os.path.isfile(CHIMEDBRC), CHIMEDBRC_MESSAGE
    os.environ["CHIMEDB_TEST_RC"] = CHIMEDBRC

    # Make sure we don't write to the actual chime database
    os.environ["CHIMEDB_TEST_ENABLE"] = "Yes, please."

    broker = Popen(["comet", "--debug", "1", "-p", PORT])

    # wait for broker start
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
    state = manager.register_state({"foo": "bar"}, "test")
    dset = manager.register_dataset(state.id, None, "test", True)
    yield (dset.id, state.id)


def test_hash(manager):
    assert isinstance(manager, Manager)

    assert hash_dictionary(A) == hash_dictionary(A)
    assert hash_dictionary(A) == hash_dictionary(B)
    assert hash_dictionary(A) != hash_dictionary(C)
    assert hash_dictionary(A) != hash_dictionary(D)
    assert hash_dictionary(A) != hash_dictionary(E)


def test_register_config(manager, broker):
    expected_config_dump = CONFIG
    expected_config_dump["type"] = "config_{}".format(__name__)

    assert expected_config_dump == manager.get_state().to_dict()


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
    assert state.to_dict() == {"foo": "bar", "type": "test"}
    assert ds.is_root is True
    # TODO: fix hash function # assert ds["state"] == manager._make_hash(state)
    assert ds.state_type == "test"


@pytest.mark.skipif(not has_chimedb, reason="No connection to chimedb")
def test_archiver(archiver, simple_ds, manager, broker):
    dset_id = simple_ds[0]
    state_id = simple_ds[1]

    # Tell chimedb where the database connection config is
    assert os.path.isfile(CHIMEDBRC), CHIMEDBRC_MESSAGE
    os.environ["CHIMEDB_TEST_RC"] = CHIMEDBRC

    # Make sure we don't write to the actual chime database
    os.environ["CHIMEDB_TEST_ENABLE"] = "foo"

    # Open database connection
    chimedb.core.connect()

    ds = chimedb.dataset.get_dataset(dset_id)

    assert ds.state.id == state_id
    assert ds.root is True

    state = chimedb.dataset.get_state(state_id)
    assert state.id == state_id
    assert state.type.name == "test"
    assert state.data == {"foo": "bar", "type": "test"}

    chimedb.core.close()


@pytest.mark.skipif(not has_chimedb, reason="No connection to chimedb")
def test_archiver_pushback(archiver):
    r = redis.Redis("127.0.0.1", 6379)
    r.ltrim("archive_dataset", 1, 0)
    r.ltrim("archive_state", 1, 0)
    assert r.llen("archive_dataset") == 0
    assert r.llen("archive_state") == 0

    # remove from redis and DB to make this test behave the same if run twice
    chimedb.dataset.Dataset.delete().where(
        chimedb.dataset.Dataset.id == "test_ds"
    ).execute()
    chimedb.dataset.DatasetState.delete().where(
        chimedb.dataset.DatasetState.id == "test_state"
    ).execute()
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
    llen = r.llen("archive_dataset")
    assert llen == 1 or llen == 0
    llen = r.llen("archive_state")
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
    time.sleep(0.2)
    assert r.llen("archive_dataset") == 0
    assert r.llen("archive_state") == 0


def test_status(simple_ds, manager):
    assert simple_ds[0] in manager._get_datasets()
    assert simple_ds[1] in manager._get_states()


def test_gather_update(simple_ds, manager, broker):
    root = simple_ds[0]
    state = manager.register_state({"f00": "b4r"}, "t3st")
    dset0 = manager.register_dataset(state.id, root, "test", False)
    state = manager.register_state({"f00": "br"}, "t3st")
    dset1 = manager.register_dataset(state.id, dset0.id, "test", False)
    state = manager.register_state({"f00": "b4"}, "t3st")
    dset2 = manager.register_dataset(state.id, dset1.id, "test", False)

    result = requests.post(
        "http://localhost:{}/update-datasets".format(PORT),
        json={"ds_id": dset2.id, "ts": 0, "roots": [root]},
    ).json()
    assert "datasets" in result
    assert dset0.id in result["datasets"]
    assert dset1.id in result["datasets"]
    assert dset2.id in result["datasets"]


def test_get_dataset(simple_ds, manager_new, broker):
    """Test to get a dataset from a new manager requesting it from the broker."""

    dset_id = simple_ds[0]
    state_id = simple_ds[1]

    test_ds = manager_new.get_dataset(dset_id)

    assert test_ds.state_id == state_id


def test_get_dataset_failure(manager_low_timeout, broker_low_timeout):
    """Test to get a non existent dataset from a new manager."""

    with pytest.raises(BrokerError):
        # what's the chance my wifi password is a valid dataset ID?
        manager_low_timeout.get_dataset(1234567890)


def test_get_state(simple_ds, manager_new, broker):
    """Test to get a state from a new manager requesting it from the broker."""
    dset_id = simple_ds[0]
    state_id = simple_ds[1]

    test_state = manager_new.get_state(type="test", dataset_id=dset_id)
    test_state2 = manager_new._get_state(state_id)

    assert test_state.state_type == "test"
    assert test_state.data["foo"] == "bar"
    assert test_state.to_dict() == test_state2.to_dict()


def test_get_state_failure(simple_ds, manager_new, broker):
    """Test to get a nonexistent state from a new manager."""

    test_state = manager_new.get_state(987654321)

    assert test_state is None


def test_tofrom_dict(simple_ds, manager):
    """Test to get a state from a new manager requesting it from the broker."""
    dset_id = simple_ds[0]
    state_id = simple_ds[1]

    # Dataset de-/serialization
    test_ds = manager.get_dataset(dset_id)
    dict_ = test_ds.to_dict()
    from_dict = Dataset.from_dict(dict_)
    assert from_dict.to_dict() == test_ds.to_dict()

    # State de-/serialization
    test_state = manager.get_state(type="test", dataset_id=dset_id)
    dict_ = test_state.to_dict()
    from_dict = State.from_dict(dict_)
    assert test_state.to_dict() == from_dict.to_dict()


def test_register_start_dataset_automated(manager_and_dataset, broker):
    """Test register_start option register_datasets."""

    manager, ds = manager_and_dataset
    start_state = manager.start_state
    config_state = manager.config_state

    # test state types
    assert start_state.state_type == "start_{}".format(__name__)
    assert config_state.state_type == "config_{}".format(__name__)

    # test returned dataset
    assert start_state.id == ds.state_id
    base_ds = manager.get_dataset(ds.base_dataset_id)
    assert config_state.id == base_ds.state_id
    assert base_ds.is_root

    # test state data
    assert config_state.data == CONFIG
    assert start_state.data["version"] == version
    assert datetime.strptime(start_state.data["time"], "%Y-%m-%d-%H:%M:%S.%f") == now


def test_lru_cache(broker, manager_low_timeout):
    """Test the dataset cache doesn't cache unknown datasets as None."""

    ds_id = "doesntexist"

    # Request an unknown dataset
    with pytest.raises(BrokerError):
        print(manager_low_timeout.get_dataset(ds_id))
    state = manager_low_timeout.register_state(data={"foo": "bar"}, state_type="test")
    assert state is not None
    assert isinstance(state, State)

    # Now register it (manually, to set the ID)
    ds = Dataset(
        state_id=state.id,
        state_type="test_lru_cache",
        dataset_id=ds_id,
        base_dataset_id=None,
        is_root=True,
    )
    request = {"hash": ds_id, "ds": ds.to_dict()}
    result = manager_low_timeout._send(REGISTER_DATASET, request)
    assert result is not None
    assert result == {"result": "success"}

    # Request again, it should exist now.
    same_ds = manager_low_timeout.get_dataset(ds_id)
    assert same_ds is not None
    assert same_ds.to_dict() == ds.to_dict()
