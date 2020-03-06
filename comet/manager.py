"""CoMeT dataset manager."""

import copy
import datetime
import inspect
import logging
import json

import requests
import mmh3

# Endpoint names:
REGISTER_STATE = "/register-state"
REGISTER_DATASET = "/register-dataset"
SEND_STATE = "/send-state"
STATUS = "/status"
STATES = "/states"
DATASETS = "/datasets"
UPDATE_DATASETS = "/update-datasets"

TIMESTAMP_FORMAT = "%Y-%m-%d-%H:%M:%S.%f"

TIMEOUT = 60

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class CometError(BaseException):
    """Base class for all comet exceptions."""

    pass


class ManagerError(CometError):
    """There was an internal error in dataset management."""

    pass


class BrokerError(CometError):
    """There was an error registering states or datasets with the broker."""

    pass


class Manager:
    """
    CoMeT dataset manager.

    An interface for the CoMeT dataset broker. State changes are passed to the manager,
    it takes care of registering everything with the broker and keeps local copies to reduce
    network traffic.
    """

    def __init__(self, host, port, noconfig=False):
        """Set up the CoMeT dataset manager.

        Parameters
        ----------
        host : str
            Dataset broker host.
        port : int
            Dataset broker port.
        """
        self.broker = "http://{}:{}".format(host, port)
        self.config_state = None
        self.start_state = None
        self.states = dict()
        self.state_reg_time = dict()
        self.datasets = dict()

        # set this to 0 so we get a full update from the broker the first time
        self._dataset_update_timestamp = 0

        self._known_root_ds_ids = {}

    def register_start(self, start_time, version, config=None):
        """Register a startup with the broker.

        This should never be called twice with different parameters. If you want to register a
        state that may change, use :function:`register_state` instead.

        This does not attach the state to a dataset. If that's what you want to do, use
        :function:`register_state` instead.

        .. deprecated:: 2019.11
          `register_config(config)` and `register_start(start_time, version)` will be
          removed in a future version. It is replaced with `register_start(start_time,
          version, config)` to simplify the API.

        Parameters
        ----------
        start_time : :class:`datetime.datetime`
            The time in UTC when the program was started.
        version : str
            A unique string identifying the version of this software. This should include version
            tags, and if applicable git commit hashes as well as the "dirty" state of the local
            working tree.
        config : dict
            The config should be JSON-serializable, preferably a dictionary.

        Raises
        ------
        :class:`ManagerError`
            If there was an internal error in the dataset management. E.g. if this was called
            already before. This is to register a startup. Once.
        :class:`BrokerError`
            If there was an error in registering stuff with the broker.
        :class:`ConnectionError`
            If the broker can't be reached.
        """
        # Todo: deprecated:
        if config is None:
            logger.warning(
                "DEPRECATED! `register_config(config)` and `register_start(start_time, "
                "version)` will be removed in a future version. It is replaced with "
                "`register_start(start_time, version, config)` to simplify the API."
            )
        if not isinstance(start_time, datetime.datetime):
            raise ManagerError(
                "start_time needs to be of type 'datetime.datetime' (is {}).".format(
                    type(start_time).__name__
                )
            )
        if not isinstance(version, str):
            raise ManagerError(
                "version needs to be of type 'str' (is {}).".format(
                    type(start_time).__name__
                )
            )
        if self.start_state:
            raise ManagerError(
                "A startup was already registered, this can only be done once."
            )
        # Todo: deprecated:
        if config:
            if not isinstance(config, dict):
                raise ManagerError(
                    "config needs to be a dictionary (is `{}`).".format(
                        type(config).__name__
                    )
                )

            # get name of callers module
            name = inspect.getmodule(inspect.stack()[1][0]).__name__
            if name == "__main__":
                name = inspect.getmodule(inspect.stack()[1][0]).__file__
            logger.info("Registering config for {}.".format(name))

            state = copy.deepcopy(config)

            state["type"] = "config_{}".format(name)
            state_id = self._make_hash(state)

            request = {"hash": state_id}
            reply = self._send(REGISTER_STATE, request)

            # Does the broker ask for the state?
            if reply.get("request") == "get_state":
                if reply.get("hash") != state_id:
                    raise BrokerError(
                        "The broker is asking for state {} when state {} (config) "
                        "was registered.".format(reply.get("hash"), state_id)
                    )
                self._send_state(state_id, state)

            self.states[state_id] = state
            self.config_state = state_id
            self.state_reg_time[state_id] = datetime.datetime.utcnow()

        # get name of callers module
        name = inspect.getmodule(inspect.stack()[1][0]).__name__
        if name == "__main__":
            name = inspect.getmodule(inspect.stack()[1][0]).__file__
        logger.info("Registering startup for {}.".format(name))

        state = {
            "time": start_time.strftime(TIMESTAMP_FORMAT),
            "version": version,
            "type": "start_{}".format(name),
        }
        if config:
            state["config_state"] = self.config_state
        state_id = self._make_hash(state)

        request = {"hash": state_id}
        reply = self._send(REGISTER_STATE, request)

        # Does the broker ask for the state?
        if reply.get("request") == "get_state":
            if reply.get("hash") != state_id:
                raise BrokerError(
                    "The broker is asking for state {} when state {} (start) was "
                    "registered.".format(reply.get("hash"), state_id)
                )
            self._send_state(state_id, state)

        self.states[state_id] = state
        self.start_state = state_id
        self.state_reg_time[state_id] = datetime.datetime.utcnow()

        return

    def register_config(self, config):
        """Register a static config with the broker.

        This should just be called once on start. If you want to register a state that may change,
        use :function:`register_state` instead.

        This does not attach the state to a dataset. If that's what you want to do, use
        :function:`register_state` instead.

        .. deprecated:: 2019.11
          `register_config(config)` and `register_start(start_time, version)` will be
          removed in a future version. It is replaced with `register_start(start_time,
          version, config)` to simplify the API.

        Parameters
        ----------
        config : dict
            The config should be JSON-serializable, preferably a dictionary.

        Raises
        ------
        :class:`ManagerError`
            If there was an internal error in the dataset management.
        :class:`BrokerError`
            If there was an error in registering stuff with the broker.
        :class:`ConnectionError`
            If the broker can't be reached.

        """
        logger.warning(
            "DEPRECATED! `register_config(config)` and `register_start(start_time, "
            "version)` will be removed in a future version. It is replaced with "
            "`register_start(start_time, version, config)` to simplify the API."
        )
        if not isinstance(config, dict):
            raise ManagerError(
                "config needs to be a dictionary (is `{}`).".format(
                    type(config).__name__
                )
            )
        if not self.start_state:
            raise ManagerError(
                "Start has to be registered before config " "(use 'register_start()')."
            )

        # get name of callers module
        name = inspect.getmodule(inspect.stack()[1][0]).__name__
        if name == "__main__":
            name = inspect.getmodule(inspect.stack()[1][0]).__file__
        logger.info("Registering config for {}.".format(name))

        state = copy.deepcopy(config)

        state["type"] = "config_{}".format(name)
        state_id = self._make_hash(state)

        request = {"hash": state_id}
        reply = self._send(REGISTER_STATE, request)

        # Does the broker ask for the state?
        if reply.get("request") == "get_state":
            if reply.get("hash") != state_id:
                raise BrokerError(
                    "The broker is asking for state {} when state {} (config) "
                    "was registered.".format(reply.get("hash"), state_id)
                )
            self._send_state(state_id, state)

        self.states[state_id] = state
        self.config_state = state_id
        self.state_reg_time[state_id] = datetime.datetime.utcnow()

        return

    def register_state(
        self, state, state_type, dump=True, timestamp=None, state_id=None
    ):
        """Register a state with the broker.

        This does not attach the state to a dataset. (yet!)

        Parameters
        ----------
        state : dict
            The state should be a JSON-serializable dictionary.
        state_type : str
            The name of the state type (e.g. "inputs", "metadata"). Be careful not choosing
            something used elsewhere.
        dump : bool
            Tells the broker if the state should be dumped to file or not.
        timestamp : str
            Overwrite the timestamp attached to this state. Should have the format
            `comet.manager.TIMESTAMP_FORMAT`. If this is `None`, the broker will use the current
            time. Only supply this if you know what you're doing. This is for example to resend
            previously dumped states to the broker again after a crash.
        state_id : int
            Manually set the hash of this state instead of letting comet compute it. Set this only
            if you know what you are doing. This is for example to resend states originally hashed
            and registered by kotekan after a failure or restart. Default: None.

        Raises
        ------
        :class:`ManagerError`
            If there was an internal error in the dataset management.
        :class:`BrokerError`
            If there was an error in registering stuff with the broker.
        :class:`ConnectionError`
            If the broker can't be reached.

        """
        if not (isinstance(state, dict) or state is None):
            raise ManagerError(
                "state needs to be a dictionary (is `{}`).".format(type(state).__name__)
            )
        if not self.start_state:
            raise ManagerError(
                "Start has to be registered before anything else "
                "(use 'register_start()')."
            )

        state = copy.deepcopy(state)

        if state_type:
            state["type"] = state_type
        elif state:
            state_type = state["type"]

        if state_id is None:
            state_id = self._make_hash(state)

        request = {"hash": state_id, "dump": dump}
        if timestamp:
            request["time"] = timestamp
        reply = self._send(REGISTER_STATE, request)

        # Does the broker ask for the state?
        if reply.get("request") == "get_state":
            if reply.get("hash") != state_id:
                raise BrokerError(
                    "The broker is asking for state {} when state {} ({}) was "
                    "registered.".format(reply.get("hash"), state_id, state_type)
                )
            self._send_state(state_id, state, dump)

        self.states[state_id] = state
        self.state_reg_time[state_id] = datetime.datetime.utcnow()

        return state_id

    def register_dataset(
        self, state, base_ds, type, root=False, dump=True, timestamp=None, ds_id=None
    ):
        """Register a dataset with the broker.

        Parameters
        ----------
        state : int
            Hash / state ID of the state attached to this dataset.
        base_ds : int
            Hash / dataset ID of the base dataset or `None` if this is a root dataset.
        type : str
            State type name of this state.
        root : bool
            `True` if this is a root dataset (default `False`).
        dump : bool
            Tells the broker if the state should be dumped to file or not.
        timestamp : str
            Overwrite the timestamp attached to this dataset. Should have the format
            `comet.manager.TIMESTAMP_FORMAT`. If this is `None`, the broker will use the current
            time. Only supply this if you know what you're doing. This is for example to resend
            previously dumped datasets to the broker again after a crash.
        ds_id : int
            Manually set the hash of this dataset instead of letting comet compute it. Set this
            only if you know what you are doing. This is for example to resend states originally
            hashed and registered by kotekan after a failure or restart. Default: None.

        Raises
        ------
        :class:`ManagerError`
            If there was an internal error in the dataset management.
        :class:`BrokerError`
            If there was an error in registering stuff with the broker.
        :class:`ConnectionError`
            If the broker can't be reached.

        """
        if not isinstance(state, str):
            raise ManagerError(
                "state needs to be a hash (str) (is `{}`).".format(type(state).__name__)
            )
        if not self.start_state:
            raise ManagerError(
                "Start has to be registered before anything else "
                "(use 'register_start()')."
            )

        ds = {"is_root": root, "state": state, "type": type}
        if base_ds is not None:
            ds["base_dset"] = base_ds

        if ds_id is None:
            ds_id = self._make_hash(ds)

        request = {"hash": ds_id, "ds": ds, "dump": dump}
        if timestamp:
            request["time"] = timestamp
        self._send(REGISTER_DATASET, request)

        self.datasets[ds_id] = ds

        return ds_id

    def _send(self, endpoint, data, rtype="post"):
        command = getattr(requests, rtype)
        try:
            reply = command(
                self.broker + endpoint, data=json.dumps(data), timeout=TIMEOUT
            )
            reply.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise BrokerError(
                "Failure connecting to comet.broker at {}{} (make sure it's running): {}".format(
                    self.broker, endpoint, err
                )
            )
        except requests.exceptions.ConnectionError:
            raise BrokerError(
                "Failure connecting to comet.broker at {}{}: make sure it is "
                "running.".format(self.broker, endpoint)
            )
        except requests.exceptions.ReadTimeout:
            raise BrokerError(
                "Timeout when connecting to comet.broker at {}{}: make sure it is "
                "running.".format(self.broker, endpoint)
            )
        reply = reply.json()
        self._check_result(reply.get("result"), endpoint)
        return reply

    def _send_state(self, state_id, state, dump=True):
        logger.debug("sending state {}".format(state_id))

        request = {"hash": state_id, "state": state, "dump": dump}
        self._send(SEND_STATE, request)

    def _check_result(self, result, endpoint):
        if result != "success":
            raise BrokerError(
                "The {}{} answered with result `{}`, expected `success`.".format(
                    self.broker, endpoint, result
                )
            )

    @staticmethod
    def _make_hash(data):
        return "%032x" % mmh3.hash128(json.dumps(data, sort_keys=True), seed=1420)

    def get_state(self, type=None, dataset_id=None):
        """
        Get the static config or the dataset state that was added last.

        If called without parameters, this returns the static config that was registered.

        Note
        ----
        This only finds state that where registered locally with the comet manager.

        Todo
        ----
        Ask the broker for unknown states.

        Parameters
        ----------
        type : str
            The name of the type of the state to return. If this is `None`, the initial config will
            be returned. Default: `None`.
        dataset_id : int
            The ID of the dataset the last state of given type should be returned for. If this is
            `None` the dataset hirarchy is assumed to not have multiple branches. Default: `None`.

        Returns
        -------
        dict
            The last config or state of requested type that was registered. Returns `None` if
            requested state not found.
        """
        if dataset_id is not None:
            raise ManagerError(
                "get_state: Not implemented for dataset_id other than None."
            )

        if type is None:
            return self.states[self.config_state]

        states_of_type = list()
        for state_id in self.states:
            state = self.states[state_id]
            if state["type"] == type:
                states_of_type.append(state)

        if states_of_type:
            states_of_type.sort(key=lambda s: self.state_reg_time[state_id])
            return states_of_type[-1]
        return None

    def get_dataset(self, dataset_id=None):
        """
        Get a dataset.

        If called without parameters, this returns the dataset added last.

        If the requested dataset is not known locally an update from the comet broker
        is requested. If the broker also doesn't know the dataset, `None` is returned.

        Parameters
        ----------
        dataset_id : int
            The ID of the dataset that should be returned. If this is `None` the dataset added last
            is returned. Default: `None`.

        Returns
        -------
        dict
            The requested dataset. Returns `None` if requested dataset not found.
        """
        try:
            return self.datasets[dataset_id]
        except KeyError:
            self._update_datasets(dataset_id)
            return self.datasets.get(dataset_id, None)

    def _update_datasets(self, dataset_id):
        """
        Get dataset update from broker.

        Gets an update on new datasets known to the broker since the last update.

        Parameters
        ----------
        dataset_id : int
            ID of the dataset to get an update for. The update will be limited to
            datasets with the same root, in case the root is known already.

        Raises
        ------
        BrokerError
            If there was an error when communicating with the broker.
        """
        logger.debug("Requesting dataset update for ID {}...".format(dataset_id))
        data = {
            "ts": self._dataset_update_timestamp,
            "ds_id": dataset_id,
            "roots": self._known_root_ds_ids,
        }
        reply = self._send(UPDATE_DATASETS, data, "post")

        # save update timestamp for next time
        self._dataset_update_timestamp = reply["ts"]

        # look for root datasets
        for id, ds in reply["datasets"].items():
            if ds["is_root"]:
                self._known_root_ds_ids["id"] = ds

        # save new datasets
        self.datasets.update(reply["datasets"])

    def broker_status(self):
        """
        Get dataset broker status.

        Returns
        -------
        bool
            True if broker is running.
        """
        response = self._send(STATUS, None, "get")
        if "running" not in response:
            return False
        return response["running"]

    def _get_states(self):
        """
        Get all state IDs known to dataset broker.

        Returns
        -------
        List[int]
            All state IDs known to dataset broker.
        """
        response = self._send(STATES, None, "get")
        return response["states"]

    def _get_datasets(self):
        """
        Get all dataset IDs known to dataset broker.

        Returns
        -------
        List[int]
            All dataset IDs known to dataset broker.
        """
        response = self._send(DATASETS, None, "get")
        return response["datasets"]
