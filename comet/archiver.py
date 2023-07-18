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
    """Main class to run the comet archiver.

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
                f"Comet archiver failed registering its startup and initial config: {exc}"
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
        logger.info(f"Started CoMeT Archiver {__version__}.")

        # names of the lists we are monitoring on redis
        TYPES = ["archive_state", "archive_dataset"]

        while True:
            # Randomly choose a type, otherwise the archiver can get stuck on a
            # single invalid item in the first list, ignoring any items in other
            # lists
            which_list = random.choice(TYPES)
            # Do not remove from the list until it has been added to the database
            data = self.redis.lrange(which_list, 0, 0)

            if not data:
                # Wait for a moment
                time.sleep(self.failure_wait_time)
                continue

            if self._exists(data, which_list):
                # Remove it from the redis list since it already exists
                # in the database
                self.redis.lpop(which_list)
                continue

            # Try to add the next item to the database
            try:
                id, timestamp = self._load_json(data)
                if which_list == "archive_state":
                    result = self._insert_state(id, timestamp)
                else:
                    result = self._insert_dataset(id, timestamp)
            except chimedb.ConnectionError as err:
                # Wait a bit longer
                logger.error(
                    "Could not connect to the chime database. The archiver will restart. \n"
                )
                raise err
            except Exception as err:
                logger.error(
                    "An unexpected error occured while adding an item to the database. \n"
                    f"{err}"
                )
                time.sleep(self.failure_wait_time)
                continue

            if not result:
                # The item wasn't added for some reason. Move it
                # to the end of the list in case it depends on something
                # elsewhere in the list
                self.redis.lmove(which_list, which_list, "LEFT", "RIGHT")

    @staticmethod
    def _exists(data, type_):
        """Check if a dataset or state exists."""
        json_data = json.loads(data)

        try:
            id = json_data["hash"]
        except KeyError as key:
            logger.error(f"Key {key} not found in data {json_data}")
            return False

        if type_ == "archive_state":
            return db.DatasetState.exists(id)

        return db.Dataset.from_id(id) is not None

    @staticmethod
    def _load_json(data):
        """Load and parse a json dataset."""
        json_data = json.loads(data)

        # The "dataset" in redis consists of just a hash and a timestamp
        try:
            id = json_data["hash"]
            time = json_data["time"]
        except KeyError as key:
            logger.error(f"Key {key} not found in data {json_data}")
            return None

        try:
            timestamp = datetime.datetime.strptime(time, TIMESTAMP_FORMAT)
        except ValueError as err:
            logger.error(f"Failure parsing timestamp {time}: {err}")
            return None

        return id, timestamp

    def _insert_state(self, id, timestamp):
        """Insert a state into the database."""

        item = self.redis.hget("states", id)

        if item is None:
            logger.error(
                f"Failure archiving state {id}. Item is not known to broker/redis."
            )
            return False

        item = json.loads(item)
        stype = item.get("type")
        db.insert_state(id, stype, timestamp, item)

        return True

    def _insert_dataset(self, id, timestamp):
        """Insert a dataset into the database."""

        item = self.redis.hget("datasets", id)

        # This item wasn't in either list
        if item is None:
            logger.error(
                f"Failure archiving dataset {id}. Item is not known to broker/redis."
            )
            return False

        item = json.loads(item)

        base = item.get("base_dset")
        is_root = item.get("is_root", False)
        state = item["state"]

        try:
            db.insert_dataset(id, base, is_root, state, timestamp)
        except db.DatasetState.DoesNotExist:
            logger.error(
                f"Failure archiving dataset {item}. "
                "DB doesn't know the referenced state."
            )
            return False
        except db.Dataset.DoesNotExist:
            logger.error(
                f"Failure archiving dataset {item}. "
                "DB doesn't know the referenced base dataset."
            )
            return False
        except DoesNotExist as err:
            logger.error(
                f"Failure archiving dataset {item}. "
                f"DB doesn't know something that was referenced: {err}"
            )
            return False

        return True

    def __del__(self):
        """Stop the archiver."""
        chimedb.close()
