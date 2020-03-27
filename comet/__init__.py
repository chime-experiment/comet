"""CoMeT: A Config and Metadata Tracker."""
from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

from .manager import Manager
from .exception import ManagerError, BrokerError, CometError
