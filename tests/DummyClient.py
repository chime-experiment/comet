"""DummyClient for locust testing CoMeT broker."""

import time
import inspect
import random
import string
import json
import copy

import requests
import datetime
from locust import events, Locust

from comet import CometError, ManagerError, BrokerError

REGISTER_STATE = "/register-state"
REQUEST_STATE = "/request-state"
REGISTER_DATASET = "/register-dataset"
UPDATE_DATASETS = "/update-datasets"
SEND_STATE = "/send-state"

TIMESTAMP_FORMAT = "%Y-%m-%d-%H:%M:%S.%f"

TIMEOUT = 60


def stopwatch(func):
    """Decorate function to time the duration of an event for locus testing."""

    def wrapper(*args, **kwargs):
        # get task's function name
        previous_frame = inspect.currentframe().f_back
        _, _, task_name, _, _ = inspect.getframeinfo(previous_frame)

        start = time.time()
        result = None
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            total = int((time.time() - start) * 1000)
            events.request_failure.fire(
                request_type="DummyClient",
                name=task_name,
                response_time=total,
                exception=e,
            )
        else:
            total = int((time.time() - start) * 1000)
            events.request_success.fire(
                request_type="DummyClient",
                name=task_name,
                response_time=total,
                response_length=0,
            )
        return result

    return wrapper


class DummyClient:
    """
    Dummy Client that interfaces with the CoMeT dataset broker.

    Used for locust load testing.
    """

    def __init__(self, broker_host, broker_port):
        """Set up the CoMeT Dummy Client.

        Parameters
        ----------
        broker_host : str
            Dataset broker host.
        broker_port : int
            Dataset broker port.
        """
        self.broker = "http://{}:{}".format(broker_host, broker_port)

        self.start_state = None
        self.states = list()
        self.datasets = list()

    @stopwatch
    def register_start(self, start_time, version):
        """Register a startup with the broker.

        This should just be called once on start.
        This needs to be called on start.

        Parameters
        ----------
        start_time : :class:`datetime.datetime`
            The time in UTC when the program was started.
        version : str
            A unique string identifying the version of the software. This should include version
            tags, and if applicable git commit hashes as well as the "dirty" state of the local
            working tree.

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

        if not isinstance(start_time, datetime.datetime):
            raise ManagerError(
                "start_time needs to be of type `datetime.datetime` (is {}).".format(
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

        # generate name for registering
        self.name = "".join(random.choice(string.ascii_lowercase) for i in range(10))
        print("Registering startup for {}.".format(self.name))

        state = {
            "time": start_time.strftime(TIMESTAMP_FORMAT),
            "version": version,
            "type": "start_{}".format(self.name),
        }
        state_id = self._make_hash(state)

        request = {"hash": state_id}
        reply = self._send(REGISTER_STATE, request)

        # Does the broker as for the state?
        if reply.get("request") == "get_state":
            if reply.get("hash") != state_id:
                raise BrokerError(
                    "The broker is asking for state {} when state {} (start) was "
                    "registered.".format(reply.get("hash"), state_id)
                )
            self._send_state(state_id, state)

        self.states.append(state_id)
        self.start_state = state_id

        return

    @stopwatch
    def register_config(self, config):
        """Register a static config with the broker.

        This should just be called once on start.

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
            If the broker can't be reached

        """
        if not isinstance(config, dict):
            raise ManagerError(
                "config needs to be a dictionary (is `{}`).".format(
                    type(config).__name__
                )
            )
        if not self.start_state:
            raise ManagerError(
                "Start has to be registered before config (use 'register_start()')."
            )

        print("Registering config for {}.".format(self.name))

        state = copy.deepcopy(config)

        state["type"] = "config_{}".format(self.name)
        state_id = self._make_hash(state)

        request = {"hash": state_id}
        reply = self._send(REGISTER_STATE, request)

        # Does the broker ask for the state?
        if reply.get("request") == "get_state":
            if reply.get("hash") != state_id:
                raise BrokerError(
                    "The broker is asking for state {} when state {} (config) was registered.".format(
                        reply.get("hash"), state_id
                    )
                )
            self._send_state(state_id, state)

        self.states.append(state_id)
        self.config_state = state_id

        return

    @stopwatch
    def register_state(
        self, state, state_type, dump=True, timestamp=None, state_id=None
    ):
        """Register a state with the broker.

        This does not attach the state to a dataset.

        Parameters
        ----------
        state : dict
            The state should be a JSON-serializable dictionary.
        state_type : str
            The name of the state type (e.g. "inputs", "metadata").
        dump : bool
            Tells the broker if the state should be dumped to file or not.
        timestamp : str
            Overwrite the timestamp attached to this state. Should have the format
            `DummyClient.TIMESTAMP_FORMAT`. If this is `None`, the broker will use the current
            time.
        state_id : int
            Manually set the hash of this sate instead of letting comet compute it.

        Raises
        ------
        :class:`ManagerError`
            If there was an internal error in the dataset management.
        :class:`BrokerError`
            If there was an error in registering stuff with the broker.
        :class:`ConnectionError`
            If the broker can't be reached

        """
        if not (isinstance(state, dict) or state is None):
            raise ManagerError(
                "state needs to be a dictionary (is `{}`).".format(type(state).__name__)
            )
        if not self.start_state:
            raise ManagerError("Start has to be registered before anything else")

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
                    "The broker is asking for state {} when state {} ({}) was registered.".format(
                        reply.get("hash"), state_id, state_type
                    )
                )
            self._send_state(state_id, state, dump)

        self.states.append(state_id)

        return state_id

    @stopwatch
    def request_state(self, state):
        """
        Request the state with the given ID.

        Parameters
        ----------
        state : int
            Hash / state ID of the state info is requested on
        """
        request = {"id": state}
        reply = self._send(REQUEST_STATE, request)

        return

    @stopwatch
    def register_dataset(
        self, state, base_ds, types, root=False, dump=True, timestamp=None, ds_id=None
    ):
        """Register a dataset with the broker.

        Parameters
        ----------
        state : int
            Hash / state ID of the state attached to this dataset.
        base_ds : int
            Hash / dataset ID of the base dataset or `None` if this is a root dataset.
        types : list of str
            State type name(s) of this state and its inner state(s).
        root : bool
            `True` if this is a root dataset (default `False`).
        dump : bool
            Tells the broker if the state should be dumped to file or not.
        timestamp : str
            Overwrite the timestamp attached to this dataset. Should have the format `DummyClient.TIMESTAMP_FORMAT`.
        ds_id : int
            Manually set the hash of this dataset instead of letting CoMeT compute it.

        Raises
        ------
        :class:`ManagerError`
            If there was an internal error in the dataset management.
        :class:`BrokerError`
            If there was an error in registering stuff with the broker.
        :class:`ConnectionError`
            If the broker can't be reached.
        """
        if not isinstance(state, int):
            raise ManagerError(
                "state needs to be a hash (int) (is `{}`).".format(type(state).__name__)
            )
        if not self.start_state:
            raise ManagerError("Start has to be registered before anything else")

        ds = {"is_root": root, "state": state, "types": types}
        if base_ds is not None:
            ds["base_dset"] = base_ds

        if ds_id is None:
            ds_id = self._make_hash(ds)

        request = {"hash": ds_id, "ds": ds, "dump": dump}
        if timestamp:
            request["time"] = timestamp
        self._send(REGISTER_DATASET, request)

        self.datasets.append(ds_id)

        return ds_id

    @stopwatch
    def update_datasets(self, ds_id, timestamp=0, roots=[]):
        """Query for an update on the dataset.

        Requests all nodes that were after the given timestamp.

        Parameters
        ----------
        ds_id : int
            Hash for the dataset you wish to update.
        timestamp : str
            Request all nodes that were after the given timestamp.
            If 0, return all nodes.
        roots : list of int
            Hash for the given roots that are included in the returned update.
            If the root of the given dataset is not among them, all datasets with the same root as the given dataset are returned.
        """
        if not isinstance(ds_id, int):
            raise ManagerError(
                "ds_id needs to be a hash (int) (is `{}`).".format(type(state).__name__)
            )

        if not self.start_state:
            raise ManagerError("Start has to be registered before anything else")

        request = {"ds_id": ds_id, "ts": timestamp, "roots": roots}

        response = self._send(UPDATE_DATASETS, request)

        return response

    @staticmethod
    def _make_hash(data):
        return hash(json.dumps(data, sort_keys=True))

    def _send(self, endpoint, data, rtype="post"):
        command = getattr(requests, rtype)
        try:
            reply = command(
                self.broker + endpoint, data=json.dumps(data), timeout=TIMEOUT
            )
            reply.raise_for_status()
        except requests.exceptions.ConnectionError:
            raise BrokerError(
                "Failure connecting to comet.broker at {}{}: make sure it is running".format(
                    self.broker, endpoint
                )
            )
        except requests.exceptions.ReadTimeout:
            raise BrokerError(
                "Timeout when connecting to comet.broker at {}{}: make sure it is running.".format(
                    self.broker, endpoint
                )
            )

        reply = reply.json()
        return reply

    def _check_result(self, result, endpoint):
        if result != "success":
            raise BrokerError(
                "The {}{} answered with result `{}`, expected 'success'.".format(
                    self.broker, endpoint, result
                )
            )

    def _send_state(self, state_id, state, dump=True):
        print("sending state {}".format(state_id))

        request = {"hash": state_id, "state": state, "dump": dump}
        self._send(SEND_STATE, request)


class DummyClientLocust(Locust):
    """
    Dummy Client Locust class. Contains DummyClient client.

    Used for locust load testing.
    """
    def __init__(self):
        self.client = DummyClient(self.host, self.port)
        super(DummyClientLocust, self).__init__()
