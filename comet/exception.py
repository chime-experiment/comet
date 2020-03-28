"""Exception subclasses for comet."""


class CometError(BaseException):
    """Base class for all comet exceptions."""

    pass


class ManagerError(CometError):
    """There was an internal error in dataset management."""

    pass


class BrokerError(CometError):
    """There was an error registering states or datasets with the broker."""

    pass
