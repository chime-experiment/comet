import aiohttp
import asyncio
import os
import time
import pytest
from random import shuffle
import signal

from datetime import datetime
from subprocess import Popen

from comet import Manager

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
version = "0.7.1"


@pytest.fixture(scope="session", autouse=True)
def manager_and_dataset():
    manager = Manager("localhost", PORT)

    ds = manager.register_start(now, version, CONFIG, register_datasets=True)
    root_ds = manager.get_dataset(ds.id).base_dataset_id
    return manager, ds, root_ds


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


async def fetch(session, url, json):
    async with session.post(url, json=json) as response:
        return await response.text()


async def sendalot(ds, base_ds, root_ds, state, start):
    json_update_ds = {"ds_id": ds, "ts": "0", "roots": []}
    json_update_ds_with_roots = {"ds_id": ds, "ts": 0, "roots": [root_ds]}
    url_update_ds = "http://localhost:{}/update-datasets".format(PORT)

    json_register_state = {"hash": state}
    url_register_state = "http://localhost:{}/register-state".format(PORT)

    json_register_ds = {
        "hash": ds,
        "ds": {"base_dset": base_ds, "state": state, "is_root": False, "type": "fifth"},
    }
    url_register_ds = "http://localhost:{}/register-dataset".format(PORT)

    timeout = aiohttp.ClientTimeout(total=100)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [fetch(session, url_update_ds, json_update_ds) for _ in range(250)]
        tasks += [
            fetch(session, url_register_state, json_register_state) for _ in range(250)
        ]
        tasks += [fetch(session, url_register_ds, json_register_ds) for _ in range(250)]
        tasks += [
            fetch(session, url_update_ds, json_update_ds_with_roots) for _ in range(250)
        ]
        shuffle(tasks)
        try:
            return await asyncio.gather(*tasks)
        except Exception as err:
            print(str(err))
            print(time.time() - start)
            raise


def test_stress_update_datasets(manager_and_dataset, broker):
    manager, ds, root_ds_id = manager_and_dataset
    s1 = manager.register_state({"a": 1}, "first")
    s2 = manager.register_state({"a": 2}, "second")
    s3 = manager.register_state({"a": 3}, "third")
    s4 = manager.register_state({"a": 4}, "fourth")
    s5 = manager.register_state({"a": 5}, "fifth")
    ds = manager.register_dataset(s1, ds, "first")
    ds = manager.register_dataset(s2, ds, "second")
    ds = manager.register_dataset(s3, ds, "third")

    for i in range(100):
        state = manager.register_state({"foo": "bar"}, "filled_by_loop")
        ds = manager.register_dataset(state, ds, "filled_by_loop")

    base_ds = manager.register_dataset(s4, ds, "fourth")
    ds = manager.register_dataset(s5, base_ds, "fifth")

    start = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(sendalot(ds.id, base_ds.id, root_ds_id, s5.id, start))
    print(time.time() - start)
