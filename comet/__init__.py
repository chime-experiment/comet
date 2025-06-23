"""comet.

A config and metadata tracker.

Submodules
----------
.. autosummary::
    :toctree: _autosummary

    archiver
    broker
    dataset
    exception
    hash
    manager
    state
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("comet")
except PackageNotFoundError:
    # package is not installed
    pass

del version, PackageNotFoundError

from .dataset import Dataset as Dataset
from .exception import (
    ManagerError as ManagerError,
    BrokerError as BrokerError,
    CometError as CometError,
)
from .manager import Manager as Manager
from .state import State as State
