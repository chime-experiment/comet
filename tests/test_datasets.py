import json
import os
import pytest

from datetime import datetime, timedelta
from comet import Manager, ManagerError
from comet.broker import DEFAULT_PORT
from comet.manager import TIMESTAMP_FORMAT

# Some dummy states for testing:
CONFIG = {'a': 1, 'b': 'fubar'}
ABC = {'a': 0, 'b': 1, 'c': 2, 'd': 3}
A = {'a': 1, 'b': 'fubar'}
B = {'a': 1, 'b': 'fubar'}
C = {'a': 1, 'b': 'fuba'}
D = {'a': 1, 'c': 'fubar'}
E = {'a': 2, 'b': 'fubar'}
F = {'a': 1}
G = {'b': 1}
H = {'blubb': 'bla'}
J = {'meta': 'data'}


@pytest.fixture(scope='session', autouse=True)
def manager():
    return Manager('localhost', DEFAULT_PORT)


def test_hash(manager):
    assert isinstance(manager, Manager)

    assert manager._make_hash(A) == manager._make_hash(A)
    assert manager._make_hash(A) == manager._make_hash(B)
    assert manager._make_hash(A) != manager._make_hash(C)
    assert manager._make_hash(A) != manager._make_hash(D)
    assert manager._make_hash(A) != manager._make_hash(E)


def test_register_config(manager):
    now = datetime.utcnow()
    version = '0.1.1'

    with pytest.raises(ManagerError):
        manager.register_config(CONFIG)
    manager.register_start(now, version)
    manager.register_config(CONFIG)
    with pytest.raises(ManagerError):
        manager.register_start(now, version)

    assert CONFIG == manager.get_state()

    # Find the dump file
    dump_files = os.listdir("./")
    dump_files = list(filter(lambda x: x.endswith("data.dump"), dump_files))
    dump_times = [f[:-10] for f in dump_files]
    dump_times = [datetime.strptime(t, TIMESTAMP_FORMAT) for t in dump_times]
    freshest = dump_times.index(max(dump_times))

    with open(dump_files[freshest], 'r') as json_file:
        comet_start_dump = json.loads(json_file.readline())
        comet_config_dump = json.loads(json_file.readline())

        start_dump = json.loads(json_file.readline())
        config_dump = json.loads(json_file.readline())

    assert comet_start_dump['state']['type'] == 'start_comet.broker'
    assert comet_config_dump['state']['type'] == 'config_comet.broker'

    expected_start_dump = {'time': now.strftime(TIMESTAMP_FORMAT), 'version': version,
                           'type': 'start_{}'.format(__name__)}
    assert start_dump['state'] == expected_start_dump
    assert start_dump['hash'] == manager._make_hash(expected_start_dump)
    assert datetime.strptime(start_dump['time'], TIMESTAMP_FORMAT) - datetime.utcnow() < timedelta(
        minutes=1)
    assert datetime.strptime(start_dump['state']['time'],
                             TIMESTAMP_FORMAT) - datetime.utcnow() < timedelta(minutes=1)

    expected_config_dump = CONFIG
    expected_config_dump['type'] = 'config_{}'.format(__name__)

    assert config_dump['state'] == expected_config_dump
    assert datetime.strptime(
        config_dump['time'], TIMESTAMP_FORMAT
    ) - datetime.utcnow() < timedelta(minutes=1)
    assert config_dump['hash'] == manager._make_hash(expected_config_dump)
