import pytest
from comet import Manager, ManagerError
from comet.broker import DEFAULT_PORT

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
    manager.register_config(CONFIG)
    with pytest.raises(ManagerError):
        manager.register_config(CONFIG)

    assert CONFIG == manager.get_state()
