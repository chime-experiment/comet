import datetime
import inspect
import requests
import random
import string
import json
import copy
import inspect
import time

from locust import events, Locust

REGISTER_STATE = "/register-state"
REGISTER_DATASET = "/register-dataset"
UPDATE_DATASETS = "/update-datasets"
SEND_STATE = "/send-state"
STATES = "/states"
DATASETS="/datasets"

TIMESTAMP_FORMAT = "%Y-%m-%d-%H:%M:%S.%f"

TIMEOUT = 60

def stopwatch(func):
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
            events.request_failure.fire(request_type="DummyClient",
                                        name = task_name,
                                        response_time = total,
                                        exception=e)
        else:
            total = int((time.time() - start) * 1000)
            events.request_success.fire(request_type="DummyClient",
                                        name=task_name,
                                        response_time=total,
                                        response_length=0)
        return result
    return wrapper

class DummyClient:

    def __init__(self, broker_host, broker_port):
        self.broker = "http://{}:{}".format(broker_host, broker_port)
        self.start_state = None
        self.states = dict()
        self.state_reg_time = dict()
        self.datasets = dict()

    @stopwatch
    def register_start(self, start_time, version):
            '''Register a startup with the broker.
            start_time
                The time in UTC when the program was started.
            version
                A unique string identifying the version of the software.
            '''

            if self.start_state:
                raise Exception(
                        "A startup was already registered, this can only be done once."
                        )

            # get name of callers module
            name = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
            print("Registering startup for {}.".format(name))

            state = {
                    "time": start_time.strftime(TIMESTAMP_FORMAT),
                    "version": version,
                    "type": "start_{}".format(name),
                    }
            state_id = self._make_hash(state)

            request = {"hash": state_id}
            reply = self._send(REGISTER_STATE, request)

            self.states[state_id] = state
            self.start_state = state_id
            self.state_reg_time[state_id] = datetime.datetime.utcnow()

            return

    @staticmethod
    def _make_hash(data):
        return hash(json.dumps(data, sort_keys=True))

    def _send(self, endpoint, data, rtype="post"):
        command = getattr(requests, rtype)
        try:
            reply = command(
                    self.broker + endpoint, data=json.dumps(data), timeout=TIMEOUT)
            reply.raise_for_status()
        except requests.exceptions.ConnectionError:
            raise Exception("Failure connecting to comet.broker at {}{}: make sure it is running".format(self.broker, endpoint)
                    )
        except requests.exceptions.ReadTimeout:
            raise Exception("Timeout when connecting to comet.broker at {}{}: make sure it is running.".format(self.broker, endpoint))

        reply = reply.json()
        self._check_result(reply.get("result"), endpoint)
        return reply

    def _check_result(self, result, endpoint):
        if result != "success":
            raise Exception("The {}{} answered with result `{}`, expected 'success'.".format(self.broker, endpoint, result))

    @stopwatch
    def register_config(self, config):
        """Registers a static config with the broker.
        """
        if not self.start_state:
            raise Exception("Start has to be registered before config (use register_start()).")

        # get name of callers module
        name = inspect.getmodule(inspect.stack()[1][0]).__name__
        if name == "__main__":
            name = inspect.getmodule(inspect.stack()[1][0]).__file__
        print("Registering config for {}.".format(name))

        state = copy.deepcopy(config)

        state["type"] = "config_{}".format(name)
        state_id = self._make_hash(state)

        request = {"hash": state_id}
        reply = self._send(REGISTER_STATE, request)

        # Does the broker ask for the state?
        if reply.get("request") == "get_state":
            if reply.get("hash") != state_id:
                raise Exception("The broker is asking for state {} when state {} (config) was registered.".format(reply.get("hash"), state_id))
            self._send_state(state_id, state)

        self.states[state_id] = state
        self.config_state = state_id
        self.state_reg_time[state_id] = datetime.datetime.utcnow()

        return

    @stopwatch
    def register_state(
            self, state, state_type, dump=True, timestamp=None, state_id=None
            ):
        '''Register a state with the broker.
        This does not attach the state to a dataset, yet.
        '''
        if not self.start_state:
            raise Exception(
                    "Start has to be registered before anything else"
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

        if reply.get("request") == "get_state":
            if reply.get("hash") != state_id:
                raise Exception(
                        "The broker is asking for state {} when state {} ({}) was registered.".format(reply.get("hash"), state_id, state_type)
                        )
            self._send_state(state_id, state, dump)
        self.states[state_id] = state
        self.state_reg_time[state_id] = datetime.datetime.utcnow()

        return state_id

    @stopwatch
    def register_dataset(
            self, state, base_ds, types, root=False, dump=True, timestamp=None, ds_id=None
            ):
        """Register a dataset with the broker"""

        ds = {"is_root": root, "state": state, "types": types}
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

    @stopwatch
    def update_datasets(
            self, ds_id, timestamp=0, root=[]
            ):

        request = {"ds_id": ds_id, "ts": timestamp, "roots": root}

        self._send(UPDATE_DATASETS, request)

        return

    def _get_datasets(self):
        """
        Get all dataset IDs known to dataset broker.
        """
        response = self._send(DATASETS, None, "get")
        return response["datasets"]

    def _get_states(self):
        """
        Get all state IDs known to dataset broker.
        """
        response = self._send(STATES, None, "get")
        return response["states"]

    def _send_state(self, state_id, state, dump=True):
        request = {"hash": state_id, "state": state, "dump": dump}
        self._send(SEND_STATE, request)

class DummyClientLocust(Locust):
    def __init__(self):
        self.client = DummyClient(self.host, self.port)
        super(DummyClientLocust, self).__init__()
