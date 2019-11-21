import datetime
import time
import os
import json
import shutil
import signal
import string
import pytest
import tempfile
import random

from locust import TaskSet, task
from DummyClient import DummyClientLocust

from subprocess import Popen

CHIMEDBRC = os.path.join(os.getcwd(), ".chimedb_test_rc")
CHIMEDBRC_MESSAGE = "Could not find {}.".format(CHIMEDBRC)
PORT = "8000"

version = "0.1.1"

large_data = []
for i in range(2035):
    for j in range(0, 2035):
        large_data.append([i, j])


def make_small_state():
    return {
        "state": {
            "time": str(datetime.datetime.utcnow()),
            "type": "start_comet.broker",
        },
        "hash": random.getrandbits(40),
    }


def make_large_state():
    state = {"state": {"inner": {"data": large_data}}, "hash": random.getrandbits(40)}

    return state


class MyTasks(TaskSet):
    def on_start(self):
        # register root dataset
        state_id = self.client.register_state({"foo": "bar"}, "test")
        dset_id = self.client.register_dataset(state_id, None, "test", True)
        self.curr_base = dset_id

    @task(5)
    def register_small_dataset(self):
        state = make_small_state()

        state_id = self.client.register_state(state, "test")
        dset_id = self.client.register_dataset(state_id, self.curr_base, "test", False)
        self.curr_base = dset_id

    @task(1)
    def register_large_dataset(self):
        state = make_large_state()

        state_id = self.client.register_state(state, "test")
        dset_id = self.client.register_dataset(state_id, self.curr_base, "test", False)
        self.curr_base = dset_id

    @task(4)
    def update_dataset(self):
        if self.client.datasets:
            ds_id = random.choice(list(self.client.datasets))
            self.client.update_datasets(ds_id)

    @task(4)
    def request_state(self):
        if self.client.states:
            state_id = random.choice(list(self.client.states))
            self.client.request_state(state_id)


class DummyManager(DummyClientLocust):
    host = "localhost"
    port = PORT
    wait_time = lambda x: 3
    task_set = MyTasks

    def setup(self):
        assert os.path.isfile(CHIMEDBRC), CHIMEDBRC_MESSAGE
        os.environ["CHIMEDB_TEST_RC"] = CHIMEDBRC

        # Make sure that we don't write to the actual chime database
        os.environ["CHIMEDB_TEST_ENABLE"] = "Yes, please."

        # TravisCI has 2 cores available
        # change the -w flag to (# of cores available - 1)
        self.broker = Popen(
            ["comet", "--debug", "0", "-t", "2", "-p", PORT, '-w', '1']
        )
        time.sleep(5)

    def teardown(self):
        pid = self.broker.pid
        os.kill(pid, signal.SIGINT)
        self.broker.terminate()

        # Give the broker a moment to delete the .lock file
        time.sleep(0.1)
