"""
Archiver for CoMeT.

Move comet broker data from redis to a mysql database
"""

import datetime
import logging
from peewee import DoesNotExist
import orjson as json
import random
import redis
import time

import chimedb.core as chimedb
import chimedb.dataset as db

from . import Manager, CometError, __version__
from .manager import TIMESTAMP_FORMAT

logger = logging.getLogger(__name__)


class Archiver:
    """
    Main class to run the comet archiver.

    New data that should be archived is expected in a list in redis. The archiver currently
    monitors the lists `registered_dataset` and `registered_state`. If a state or dataset
    referenced in any new item, the archiver pushes the item back on the list for later.
    """

    def __init__(
        self,
        broker_host,
        broker_port,
        redis_host,
        redis_port,
        log_level,
        failure_wait_time,
    ):
        logger.setLevel(log_level)

        # convert ms to s
        self.failure_wait_time = failure_wait_time / 1000

        startup_time = datetime.datetime.utcnow()
        config = {
            "broker_host": broker_host,
            "broker_port": broker_port,
            "redis_host": redis_host,
            "redis_port": redis_port,
            "log_level": log_level,
            "failure_wait_time": failure_wait_time,
        }

        manager = Manager(broker_host, broker_port)
        try:
            manager.register_start(startup_time, __version__, config)
        except (CometError, ConnectionError) as exc:
            logger.error(
                "Comet archiver failed registering its startup and initial config: {}".format(
                    exc
                )
            )
            exit(1)

        # Open database connection
        chimedb.connect(read_write=True)

        # Create any missing table.
        chimedb.orm.create_tables("chimedb.dataset")

        # Open connection to redis
        self.redis = redis.Redis(
            redis_host, redis_port, encoding="utf-8", decode_responses=True
        )

    def run(self):
        """Run comet archiver (forever)."""
        logger.info("Started CoMeT Archiver {}.".format(__version__))

        # names of the lists we are monitoring on redis
        TYPES = ["archive_state", "archive_dataset"]

        while True:
            # Shuffle the list of types, otherwise the archiver can get stuck on a
            # single invalid item in the first list, ignoring any items in the second
            # list.
            random.shuffle(TYPES)
            try:
                what, data = self.redis.brpop(TYPES)

                if what == "archive_state":
                    self._insert_state(data)
                elif what == "archive_dataset":
                    self._insert_dataset(data)
                else:
                    logger.warning(
                        f"Unexpected key returned by BRPOP: {what} (expected one of {TYPES})."
                    )
                    self._pushback(what, data)
                    # slow down. this would turn into a busy wait otherwise...
                    time.sleep(self.failure_wait_time)
            except Exception as e1:
                logger.error(
                    f"Uncaught exception {e1}! Trying to pushback before crash."
                )
                try:
                    self._pushback(what, data)
                except Exception as e2:
                    logger.error(
                        f"Pushback failed: {e2}!. Item {data} from {what} will be missing."
                    )
                    raise e2 from e1
                raise

    def _pushback(self, listname, data):
        """
        Push the item back into the list.

        Slow down so this doesn't become a busy-wait.
        """
        llen = self.redis.lpush(listname, data)
        logger.error(
            "Pushed data back into the list {} on redis at {}:{}. Current list length is {}. Please have a look. Sleeping for {}s ...".format(
                listname,
                self.redis.connection_pool.connection_kwargs["host"],
                self.redis.connection_pool.connection_kwargs["port"],
                llen,
                self.failure_wait_time,
            )
        )
        time.sleep(self.failure_wait_time)

    def _insert_state(self, data):
        logger.info("Archiving state {}".format(data))
        json_data = json.loads(data)

        # Parse the json data. If anything goes wrong, push the state back into the list. This is someone else's problem now.
        try:
            state_id = json_data["hash"]
            time = json_data["time"]
        except KeyError as key:
            logger.error("Key {} not found in state data {}.".format(key, json_data))
            self._pushback("archive_state", data)
            return

        try:
            timestamp = datetime.datetime.strptime(time, TIMESTAMP_FORMAT)
        except ValueError as err:
            logger.error(
                "Failure parsing timestamp {} in state data: {}.".format(time, err)
            )
            self._pushback("archive_state", data)
            return

        # Get the state from redis
        state = self.redis.hget("states", state_id)
        if state is None:
            logger.error(
                "Failure archiving state {}: state is not known to broker/redis.".format(
                    state_id
                )
            )
            self._pushback("archive_state", data)
            return
        state = json.loads(state)

        state_type = state["type"]
        db.insert_state(state_id, state_type, timestamp, state)

    def _insert_dataset(self, data):
        logger.info("Archiving dataset {}".format(data))
        json_data = json.loads(data)

        # Parse the json data. If anything goes wrong, push the state back into the list. This is someone else's problem now.
        try:
            ds_id = json_data["hash"]
            time = json_data["time"]
        except KeyError as key:
            logger.error("Key {} not found in dataset data {}.".format(key, json_data))
            self._pushback("archive_dataset", data)
            return

        try:
            timestamp = datetime.datetime.strptime(time, TIMESTAMP_FORMAT)
        except ValueError as err:
            logger.error(
                "Failure parsing timestamp {} in dataset data: {}.".format(time, err)
            )
            self._pushback("archive_dataset", data)
            return

        # Get the dataset from redis
        dataset = self.redis.hget("datasets", ds_id)
        if dataset is None:
            logger.error(
                "Failure archiving dataset (dataset is not known to broker/redis): {}".format(
                    ds_id
                )
            )
            self._pushback("archive_dataset", data)
            return
        dataset = json.loads(dataset)

        base_dset = dataset.get("base_dset", None)
        is_root = dataset.get("is_root", False)
        state = dataset["state"]

        try:
            db.insert_dataset(ds_id, base_dset, is_root, state, timestamp)
        except db.DatasetState.DoesNotExist:
            logger.error(
                "Failure archiving dataset (DB doesn't know the referenced state): {}".format(
                    data
                )
            )
            self._pushback("archive_dataset", data)
            return
        except db.Dataset.DoesNotExist:
            logger.error(
                "Failure archiving dataset (DB doesn't know the referenced base dataset): {}".format(
                    data
                )
            )
            self._pushback("archive_dataset", data)
            return
        # This is because these peewee exception names sometimes change somehow?
        except DoesNotExist as err:
            logger.error(
                "Failure archiving dataset {} (DB doesn't know something referenced by it): {}".format(
                    data, err
                )
            )
            self._pushback("archive_dataset", data)
            return

    def __del__(self):
        """Stop the archiver."""
        chimedb.close()
