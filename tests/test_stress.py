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

CHIMEDBRC = os.path.join(os.getcwd() + ".chimedb_test_rc")
COMETTESTDATA = os.path.join(os.getcwd() + "comet-data")
CHIMEDBRC_MESSAGE = "Could not find {}.".format(CHIMEDBRC)
PORT = "8000"

CONFIG = {"a": 1, "b": "fubar"}
now = datetime.datetime.utcnow()
version = "0.1.1"
directory = tempfile.mkdtemp()

class MyTasks(TaskSet):

    def on_start(self):
        self.client.register_start(now, version)
        self.client.register_config(CONFIG)
        self.states = os.listdir(COMETTESTDATA)

    @task(3)
    def complex_ds(self):
        state_file = os.path.join(COMETTESTDATA, random.choice(self.states))
        with open(state_file) as sf:
            states_sf = sf.readlines()
            state = json.loads(random.choice(states_sf))

        state_id = self.client.register_state(state, "test")
        self.client.register_dataset(state_id, None, ["test"], True)

    @task(1)
    def simple_ds(self):
        state_id = self.client.register_state({"foo": "bar"}, "test")
        dset_id = self.client.register_dataset(state_id, None, ["test"], True)

    @task(1)
    def update_dataset(self):
        if self.client.datasets:
            ds_id = random.choice(list(self.client.datasets.keys()))
            self.client.update_datasets(ds_id)

class DummyManager(DummyClientLocust):
    host = "localhost"
    port = PORT
    wait_time = 3
    task_set = MyTasks

    def setup(self):
        assert os.path.isfile(CHIMEDBRC), CHIMEDBRC_MESSAGE
        os.environ["CHIMEDB_TEST_RC"] = CHIMEDBRC

        # Make sure that we don't write to the actual chime database
        os.environ["CHIMEDB_TEST_ENABLE"] = "Yes, please."

        self.broker = Popen(["comet", "--debug", "1", "-d", directory, "-t", "2", "-p", PORT])
        time.sleep(5)

    def teardown(self):
        pid = self.broker.pid
        os.kill(pid, signal.SIGINT)
        self.broker.terminate()

        # Give the broker a moment to delete the .lock file
        time.sleep(0.1)
        shutil.rmtree(directory)


