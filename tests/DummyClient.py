"""DummyClient for locust testing CoMeT broker."""

import time
import inspect
import random
import string
import json
import copy

import requests
import datetime
import mmh3
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
        self.states = list()
        self.datasets = list()

    @stopwatch
    def register_state(self, state, state_type):
        """Register a state with the broker.

        This does not attach the state to a dataset.

        Parameters
        ----------
        state : dict
            The state should be a JSON-serializable dictionary.
        state_type : str

        Raises
        ------
        :class:`ManagerError`
            If there was an internal error in the dataset management.
        :class:`BrokerError`
            If there was an error in registering stuff with the broker.
        :class:`ConnectionError`
            If the broker can't be reached

        """

        if state_type:
            state["type"] = state_type

        state_id = self._make_hash(state)

        request = {"hash": state_id, "dump": True}
        reply = self._send(REGISTER_STATE, request)

        # Does the broker ask for the state?
        if reply.get("request") == "get_state":
            self._send_state(state_id, state, request["dump"])

        self.states.append(state_id)

        return state_id

    @stopwatch
    def request_state(self, state_id):
        """
        Request the state with the given ID.

        Parameters
        ----------
        state : str
            Hash / state ID of the state info is requested on
        """
        request = {"id": state_id}
        reply = self._send(REQUEST_STATE, request)

        return

    @stopwatch
    def register_dataset(self, state, base_ds, state_type, root=False):
        """Register a dataset with the broker.

        Parameters
        ----------
        state : int
            Hash / state ID of the state attached to this dataset.
        base_ds : int
            Hash / dataset ID of the base dataset or `None` if this is a root dataset.
        state_type : str
            State type name of this state.
        root : bool
            `True` if this is a root dataset (default `False`).

        Raises
        ------
        :class:`ManagerError`
            If there was an internal error in the dataset management.
        :class:`BrokerError`
            If there was an error in registering stuff with the broker.
        :class:`ConnectionError`
            If the broker can't be reached.
        """
        ds = {"is_root": root, "state": state, "type": state_type}

        if base_ds is not None:
            ds["base_dset"] = base_ds

        ds_id = self._make_hash(ds)

        request = {"hash": ds_id, "ds": ds, "dump": True}
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

        request = {"ds_id": ds_id, "ts": timestamp, "roots": roots}

        response = self._send(UPDATE_DATASETS, request)

        return response

    @staticmethod
    def _make_hash(data):
        return mmh3.hash_bytes(json.dumps(data, sort_keys=True)).hex()

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
