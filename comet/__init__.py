"""CoMeT: A Config and Metadata Tracker."""
from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

from .dataset import Dataset
from .exception import ManagerError, BrokerError, CometError
from .manager import Manager
from .state import State
