"""CoMeT: A Config and Metadata Tracker."""
from .version import __version__
from .manager import Manager, ManagerError, BrokerError, CometError

# The broker uses asyncio which doesn't exist in python2. However we want to be able to use the
# manager in python2, so make the Broker import dependent on the python version.
from sys import version_info

if version_info.major > 2:
    from .broker import Broker
    from .dumper import Dumper
    from .archiver import Archiver
