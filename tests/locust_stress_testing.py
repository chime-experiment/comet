"""
Code for running stress tests using locust.

https://docs.locust.io/en/latest/installation.html

To run tests, run `locust -f tests/locust_stress_testing.py` on the commandline.
Code for the locust client is located in tests/DummyClient.py.
"""
import datetime
import time
import os
import json
import shutil
import signal
import string
import tempfile
import random
from subprocess import Popen

import pytest
from locust import TaskSet, task

from DummyClient import DummyClientLocust


CHIMEDBRC = os.path.join(os.getcwd(), ".chimedb_test_rc")
PORT = "8000"
HOST = "localhost"
version = "0.1.1"


def make_small_state():
    """Build a small state for sample_data."""
    return {
        "state": {
            "time": str(datetime.datetime.utcnow()),
            "type": "start_comet.broker",
        },
        "hash": random.getrandbits(40),
    }


large_data = []
for i in range(2035):
    for j in range(0, 2035):
        large_data.append([i, j])


def make_large_state():
    """Build a large state for sample_data."""
    state = {"state": {"inner": {"data": large_data}}, "hash": random.getrandbits(40)}

    return state


class MyTasks(TaskSet):
    """Set of tasks for testing comet broker.

    To add a task, create a function and decorate it with @task(n)
    where the higher n is the more frequently that task will be executed.

    Attributes
    ----------
    client : class:DummyClient:
        An instance of the DummyClient that represents one theoretical manager.
    curr_base : str
        The hash for the current base dataset of the dataset tree.
        For /update-dataset to be more stressfull for comet,
        we need to build dependencies between datasets: so we create one and
        use it as a base dataset for the next one.
    """

    def on_start(self):
        """Run upon initialising each individual client."""
        # register root dataset
        root_state_id = self.client.register_state({"foo": "bar"}, "test")
        root_dset_id = self.client.register_dataset(root_state_id, None, "test", True)
        self.curr_base = root_dset_id

    @task(5)
    def register_small_dataset(self):
        """
        Register a small dataset with the CoMeT broker.

        Associate it with the current base dataset.
        It is now the new base dataset.
        """
        state = make_small_state()

        state_id = self.client.register_state(state, "test")
        dset_id = self.client.register_dataset(state_id, self.curr_base, "test", False)
        self.curr_base = dset_id

    @task(1)
    def register_large_dataset(self):
        """
        Register a large dataset with the CoMeT broker.

        Associate it with the current base dataset.
        It is now the new base dataset.
        """
        state = make_large_state()

        state_id = self.client.register_state(state, "test")
        dset_id = self.client.register_dataset(state_id, self.curr_base, "test", False)
        self.curr_base = dset_id

    @task(4)
    def update_dataset(self):
        """Make a call to /update-dataset using a randomly chosen registered dataset."""

        if self.client.datasets:
            ds_id = random.choice(list(self.client.datasets))
            self.client.update_datasets(ds_id)

    @task(4)
    def request_state(self):
        """Make a call to /request-state using a randomly chosen registered state."""
        if self.client.states:
            state_id = random.choice(list(self.client.states))
            self.client.request_state(state_id)


class DummyManager(DummyClientLocust):
    """
    Configuration for the CoMeT locust tests.

    Attributes
    ----------
    host : str
        hostname for the CoMeT broker
    port : str
        port for the CoMeT broker
    wait_time : func
        each locust instance uses this to calculate how long to idle
        after a particular request
    task_set : :class:TaskSet
        set of tasks that define the locusts behaviour
    """

    host = HOST
    port = PORT
    wait_time = lambda x: 20
    task_set = MyTasks

    def setup(self):
        """Set-up operations before any of the locusts start up."""
        assert os.path.isfile(CHIMEDBRC), "Could not find {}.".format(CHIMEDBRC)
        os.environ["CHIMEDB_TEST_RC"] = CHIMEDBRC

        # Make sure that we don't write to the actual chime database
        os.environ["CHIMEDB_TEST_ENABLE"] = "Yes, please."

        # TravisCI has 2 cores available
        # change the -w flag to (# of cores available - 1)
        self.broker = Popen(["comet", "--debug", "0", "-t", "2", "-p", PORT, "-w", "1"])
        time.sleep(5)

    def teardown(self):
        """Teardown operation at the very, very end of test execution."""
        pid = self.broker.pid
        os.kill(pid, signal.SIGINT)
        self.broker.terminate()

        # Give the broker a moment to delete the .lock file
        time.sleep(0.1)
