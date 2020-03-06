"""REST Server for CoMeT (the Dataset Broker)."""
import aioredis
import asyncio
import datetime
import json
import random
import logging
import redis as redis_sync
import time

from bisect import bisect_left
from caput import time as caput_time
from math import ceil
from socket import socket
from threading import Thread

import contextvars
from sanic import Sanic
from sanic import response
from sanic.log import logger

from . import __version__
from .manager import Manager, CometError, TIMESTAMP_FORMAT


REQUESTED_STATE_TIMEOUT = 35
DEFAULT_PORT = 12050
REDIS_SERVER = ("localhost", 6379)

# config variable
wait_time = None

app = Sanic(__name__)
app.config.REQUEST_TIMEOUT = 120
app.config.RESPONSE_TIMEOUT = 120

lock_datasets = None
lock_states = None

waiting_datasets = {}
waiting_states = {}

# will have a unique value for every request sent to the broker
# used for tracking sequences of events in the logs
request_id = contextvars.ContextVar("request_id", default=0)


class RequestFormatter(logging.Formatter):
    """Logging formatter that adds a request_id to the logger's record.

    Parameters
    ----------
    request_id : ContextVar
        A context variable that contains the request ID.
    format : str
        A format string which represents how the log should be formatted.
    """

    def __init__(self, request_id=0, format=format):
        self.request_id = request_id
        super().__init__(format)

    def format(self, record):
        """Return a formatted string for the log.

        Set the record.request_id.

        Parameters
        ----------
        record : dict
            A set of relevant attributes of the logged message
            that are parsed into the format string.
        """
        record.request_id = self.request_id.get()
        return logging.Formatter.format(self, record)


"""
This associates an id, unique for every request thread, with the log formatter.
Every request has its request id integrated into its logging,
without anything required of the developer or at the time of writing the log msg.
Formatters have to be set on log handlers, and then log handlers are added to loggers.
"""
logger = logging.getLogger(__name__)
syslog = logging.StreamHandler()
formatter = RequestFormatter(
    request_id,
    "%(asctime)s [%(process)d] [%(name)s:%(levelname)s] [request=%(request_id)s] %(message)s",
)
syslog.setFormatter(formatter)
logging.getLogger("comet").setLevel(logging.DEBUG)
logging.getLogger("").addHandler(syslog)


@app.middleware("request")
async def set_request_id(request):
    """Set a unique ID for each request."""
    request_id.set(random.getrandbits(40))


@app.route("/status", methods=["GET"])
async def status(request):
    """
    Get status of CoMeT (dataset-broker).

    Poke comet to see if it's alive. Is either dead or returns {"running": True}.

    curl -X GET http://localhost:12050/status
    """
    try:
        logger.debug("status: Received status request")
        return response.json({"running": True, "result": "success"})
    except Exception as e:
        logger.error("status: received exception %s", e)
        raise
    finally:
        logger.debug("status: finished")


@app.route("/states", methods=["GET"])
async def get_states(request):
    """
    Get states from CoMeT (dataset-broker).

    Shows all states registered by CoMeT (the broker).

    curl -X GET http://localhost:12050/states
    """

    try:
        logger.debug("get_states: Received states request")

        states = await redis.execute("hkeys", "states")
        reply = {"result": "success", "states": states}

        logger.debug("states: {}".format(states))
        return response.json(reply)
    except Exception as e:
        logger.error("get_states: received exception %s", e)
        raise
    finally:
        logger.debug("get_states: finished")


@app.route("/datasets", methods=["GET"])
async def get_datasets(request):
    """
    Get datasets from CoMeT (dataset-broker).

    Shows all datasets registered by CoMeT (the broker).

    curl -X GET http://localhost:12050/datasets
    """
    try:
        logger.debug("get_datasets: Received datasets request")

        datasets = await redis.execute("hkeys", "datasets")
        reply = {"result": "success", "datasets": datasets}

        logger.debug("datasets: {}".format(datasets))
        return response.json(reply)
    except Exception as e:
        logger.error("get_datasets: received exception %s", e)
        raise
    finally:
        logger.debug("get_datasets: finished")


async def archive(data_type, json_data):
    """
    Add a state or dataset to the list for the archiver.

    Parameters
    ----------
    data_type : str
        One of "dataset" or "state".
    json_data : dict
        Should contain the field `hash` (`str`). Optionally it can contain the field
        `time` (`str`), otherwise the broker will use the current time. Should have the
         format `comet.manager.TIMESTAMP_FORMAT`.

    Raises
    ------
    CometError
        If the parameters are not as described above.
    """
    logger.debug("Passing {} to archiver.".format(data_type))

    # currently known types to be used here
    TYPES = ["dataset", "state"]

    # check parameters
    if not isinstance(data_type, str):
        raise CometError(
            "Expected string for type to send to archiver (was {}).",
            format(type(data_type)),
        )
    if data_type not in TYPES:
        raise CometError(
            "Expected one of {} for type to send to archiver (was {}).",
            format(TYPES, data_type),
        )
    if "hash" not in json_data:
        raise CometError("No hash found in json_data: {}".format(json_data))
    if not isinstance(json_data["hash"], str) and not isinstance(
        json_data["hash"], int
    ):
        raise CometError(
            "Expected type str for hash in json_data (was {})".format(
                type(json_data["hash"])
            )
        )
    if "time" not in json_data:
        json_data["time"] = datetime.datetime.utcnow().strftime(TIMESTAMP_FORMAT)
    else:
        if not isinstance(json_data["time"], str):
            raise CometError(
                "Expected type str for time in json_data (was {})".format(
                    type(json_data["time"])
                )
            )
    # push it into list for archiver
    await redis.execute(
        "lpush",
        "archive_{}".format(data_type),
        json.dumps({"hash": json_data["hash"], "time": json_data["time"]}),
    )


@app.route("/register-state", methods=["POST"])
async def register_state(request):
    """Register a dataset state with the comet broker.

    This should only ever be called by kotekan's datasetManager.
    """
    try:
        hash = request.json["hash"]
        logger.info("/register-state {}".format(hash))
        reply = dict(result="success")

        # Lock states and check if the received state is already known.
        async with lock_states:
            state = await redis.execute("hget", "states", hash)
            if state is None:
                # we don't know this state, did we request it already?
                # After REQUEST_STATE_TIMEOUT we request it again.
                request_time = await redis.execute("hget", "requested_states", hash)
                if request_time:
                    request_time = float(request_time)
                    if request_time > time.time() - REQUESTED_STATE_TIMEOUT:
                        return response.json(reply)
                    else:
                        logger.debug(
                            "register-state: {} requested {:.2f}s ago, asking again....".format(
                                hash, time.time() - request_time
                            )
                        )

                # otherwise, request it now
                reply["request"] = "get_state"
                reply["hash"] = hash
                logger.debug("register-state: Asking for state, hash: {}".format(hash))
                await redis.execute("hset", "requested_states", hash, time.time())
        return response.json(reply)
    except Exception as e:
        logger.error("register-state: received exception %s", e)
        raise
    finally:
        logger.debug("register-state: finished")


@app.route("/send-state", methods=["POST"])
async def send_state(request):
    """Send a dataset state to CoMeT (the broker).

    This should only ever be called by kotekan's datasetManager.
    """
    try:
        hash = request.json["hash"]
        state = request.json["state"]
        if state:
            type = state["type"]
        else:
            type = None
        logger.info("/send-state {} {}".format(type, hash))
        reply = dict()
        archive_state = False

        # In case the shielded part of this endpoint gets cancelled, we ignore it but
        # re-raise the CancelledError in the end
        cancelled = None

        # Lock states and check if we know this state already.
        async with lock_states:
            found = await redis.execute("hget", "states", hash)
            if found is not None:
                # this string needs to be deserialized, contains a state
                found = json.loads(found)

                # if we know it already, does it differ?
                if found != state:
                    reply["result"] = (
                        "error: hash collision ({})\nTrying to register the following "
                        "dataset state:\n{},\nbut a different state is know to "
                        "the broker with the same hash:\n{}".format(hash, state, found)
                    )
                    logger.warning("send-state: {}".format(reply["result"]))
                else:
                    reply["result"] = "success"
            else:
                await redis.execute("hset", "states", hash, json.dumps(state))
                reply["result"] = "success"
                archive_state = True

                # Notify anything waiting for this state to arrive
                signal_created(hash, "state", lock_states, waiting_states)

        # Remove it from the set of requested states (if it's in there.)
        try:
            await asyncio.shield(redis.execute("hdel", "requested_states", hash))
        except asyncio.CancelledError as err:
            logger.info(
                "/send-state {}: Cancelled while removing requested state. Ignoring...".format(
                    hash
                )
            )
            cancelled = err

        if archive_state:
            await asyncio.shield(archive("state", request.json))

        # Done cleaning up, re-raise if this request got cancelled.
        if cancelled:
            raise cancelled
        return response.json(reply)
    except Exception as e:
        logger.error("send-state: received exception %s", e)
        raise
    finally:
        logger.debug("send-state: finished")


@app.route("/register-dataset", methods=["POST"])
async def register_dataset(request):
    """Register a dataset with CoMeT (the broker).

    This should only ever be called by kotekan's datasetManager.
    """
    try:
        hash = request.json["hash"]
        logger.info("/register-dataset {}".format(hash))
        ds = request.json["ds"]

        dataset_valid = await check_dataset(ds)
        reply = dict()
        root = None
        if dataset_valid:
            root = await find_root(hash, ds)
        if root is None:
            reply["result"] = "Dataset {} invalid.".format(hash)
            logger.debug(
                "register-dataset: Received invalid dataset with hash {} : {}".format(
                    hash, ds
                )
            )
            return response.json(reply)

        archive_ds = False

        # In case the shielded part of this endpoint gets cancelled, we ignore it but
        # re-raise the CancelledError in the end
        cancelled = None

        # Lack datasets and check if dataset already known.
        async with lock_datasets:
            found = await redis.execute("hget", "datasets", hash)
            if found is not None:
                # this string needs to be deserialized, contains a dataset
                found = json.loads(found)
                # if we know it already, does it differ?
                if found != ds:
                    reply["result"] = (
                        "error: hash collision ({})\nTrying to register the following dataset:\n{},\nbut a different one is know to "
                        "the broker with the same hash:\n{}".format(hash, ds, found)
                    )
                    logger.warning("register-dataset: {}".format(reply["result"]))
                else:
                    reply["result"] = "success"
            elif dataset_valid and root is not None:
                # save the dataset
                await save_dataset(hash, ds, root)

                reply["result"] = "success"
                archive_ds = True

                # Notify anything waiting for this dataset to arrive
                signal_created(hash, "dataset", lock_datasets, waiting_datasets)

        if archive_ds:
            await asyncio.shield(archive("dataset", request.json))

        # Done cleaning up, re-raise if this request got cancelled.
        if cancelled:
            raise cancelled

        return response.json(reply)
    except Exception as e:
        logger.error("register-dataset: received exception %s", e)
        raise
    finally:
        logger.debug("register-dataset: finished")


async def save_dataset(hash, ds, root):
    """Save the given dataset, its hash and a current timestamp.

    This should be called while a lock on the datasets is held.
    """
    # add a timestamp to the dataset (ms precision)
    ts = caput_time.datetime_to_unix(datetime.datetime.utcnow())

    # get dicts from redis concurrently
    task = asyncio.ensure_future(redis.execute("hget", "datasets_of_root", root))
    datasets_of_root_keys = await redis.execute("hget", "datasets_of_root_keys", root)
    (task,), _ = await asyncio.wait({task})
    datasets_of_root = task.result()

    # create entry if this is the first node with that root
    if not datasets_of_root:
        datasets_of_root = list()
        datasets_of_root_keys = list()
    else:
        datasets_of_root = json.loads(datasets_of_root)
        datasets_of_root_keys = json.loads(datasets_of_root_keys)

    # Determine where to insert dataset ID.
    i = bisect_left(datasets_of_root_keys, ts)

    # Insert timestamp in keys list.
    datasets_of_root_keys.insert(i, ts)

    # Insert the dataset ID itself in the corresponding place.
    datasets_of_root.insert(i, hash)

    # save changes
    task1 = asyncio.ensure_future(
        redis.execute("hset", "datasets_of_root", root, json.dumps(datasets_of_root))
    )
    task2 = asyncio.ensure_future(
        redis.execute(
            "hset", "datasets_of_root_keys", root, json.dumps(datasets_of_root_keys)
        )
    )

    # Insert the dataset in the hashmap
    task3 = asyncio.ensure_future(
        redis.execute("hset", "datasets", hash, json.dumps(ds))
    )

    # Wait for all concurrent tasks
    await asyncio.wait({task1, task2, task3})


async def gather_update(ts, roots):
    """Gather the update for a given time and roots.

    Returns a dict of dataset ID -> dataset with all datasets with the
    given roots that were registered after the given timestamp.
    """
    update = dict()
    async with lock_datasets:
        for root in roots:
            # Get both dicts from redis concurrently:
            keys, tree = await asyncio.gather(
                redis.execute("hget", "datasets_of_root_keys", root),
                redis.execute("hget", "datasets_of_root", root),
            )

            keys = reversed(json.loads(keys))
            tree = list(reversed(json.loads(tree)))

            # The nodes in tree are ordered by their timestamp from new to
            # old, so we are done as soon as we find an older timestamp than
            # the given one.
            tasks = []
            for n, k in zip(tree, keys):
                if k < ts:
                    break
                tasks.append(
                    asyncio.ensure_future(redis.execute("hget", "datasets", n))
                )

            if tasks:
                # Wait for all concurrent tasks (gather keeps their order)
                tasks = await asyncio.gather(*tasks)
                # put back together the root ds IDs and the datasets
                update.update(dict(zip(tree, [json.loads(task) for task in tasks])))
    return update


async def find_root(hash, ds):
    """Return the dataset Id of the root of this dataset."""
    root = hash
    while not ds["is_root"]:
        root = ds["base_dset"]
        found = await wait_for_dset(root)
        if not found:
            logger.error("find_root: dataset {} not found.".format(hash))
            return None
        ds = json.loads(await redis.execute("hget", "datasets", root))
    return root


async def check_dataset(ds):
    """Check if a dataset is valid.

    For a dataset to be valid, the state and base dataset it references to
    have to exist. If it is a root dataset, the base dataset does not have
    to exist.
    """
    logger.debug("check_dataset: Checking dataset: {}".format(ds))
    found = await wait_for_state(ds["state"])
    if not found:
        logger.debug("check_dataset: State of dataset unknown: {}".format(ds))
        return False
    if ds["is_root"]:
        logger.debug("check_dataset: dataset {} OK".format(ds))
        return True
    found = await wait_for_dset(ds["base_dset"])
    if not found:
        logger.debug("check_dataset: Base dataset of dataset unknown: {}".format(ds))
        return False
    return True


@app.route("/request-state", methods=["POST"])
async def request_state(request):
    """Request the state with the given ID.

    This is called by kotekan's datasetManager.

    curl -d '{"state_id":42}' -X POST -H "Content-Type: application/json"
         http://localhost:12050/request-state
    """
    try:
        id = request.json["id"]
        logger.debug("/request-state {}".format(id))

        reply = dict()
        reply["id"] = id

        # Do we know this state ID?
        logger.debug("request-state: waiting for state ID {}".format(id))
        found = await wait_for_state(id)
        if not found:
            reply["result"] = "state ID {} unknown to broker.".format(id)
            logger.info("request-state: State {} unknown to broker".format(id))
            return response.json(reply)
        logger.debug("request-state: found state ID {}".format(id))

        reply["state"] = json.loads(await redis.execute("hget", "states", id))

        reply["result"] = "success"
        logger.debug("request-state: Replying with state {}".format(id))
        return response.json(reply)
    except Exception as e:
        logger.error("request-state: received exception %s", e)
        raise
    finally:
        logger.debug("request-state: finished")


def signal_created(id, name, lock, event_dict):
    """Signal when an object has been created in redis.

    Parameters
    ----------
    id : str
        Hash key name.
    name : str
        Name of object type.
    lock : asyncio.Lock
        Lock to protect event creation/signalling.
    event_dict : dict
        A dictionary to find events for signalling creation.
    """

    if not lock.locked():
        raise RuntimeError(f"lock must be held when signalling {name} creation.")

    # Notify anything waiting for this dataset to arrive
    if id in event_dict:
        event_dict[id].set()
        del event_dict[id]
        logger.debug(
            f"Signalled tasks waiting on creation of {name} {id} and removed event."
        )


async def wait_for_x(id, name, lock, redis_hash, event_dict):
    """Wait until a given object is present in redis.

    Parameters
    ----------
    id : str
        Hash key name.
    name : str
        Name of object type.
    lock : asyncio.Lock
        Lock to protect event creation.
    redis_hash : str
        Name of redis hash map.
    event_dict : dict
        A dictionary to add events for signalling creation.

    Returns
    -------
    found : bool
        True if found, False if timeout first.
    """

    # Test first before acquiring the lock as it means we might not need to wait
    if await redis.execute("hexists", redis_hash, id):
        return True

    logger.debug(f"wait_for_{name}: Waiting for {name} {id}")

    async with lock:
        # While we are locked, test again to ensure that we have the dataset
        if await redis.execute("hexists", redis_hash, id):
            return True

        if id not in event_dict:
            event_dict[id] = asyncio.Event()

        wait_event = event_dict[id]

    try:
        await asyncio.wait_for(wait_event.wait(), wait_time)
    except asyncio.TimeoutError:
        logger.warning(
            f"wait_for_{name}: Timeout ({wait_time}s) when waiting for {name} {id}"
        )
        return False
    except asyncio.CancelledError:
        logger.warning(
            f"wait_for_{name}: Request cancelled when waiting for {name} {id}"
        )
        return False

    if await redis.execute("hexists", redis_hash, id):
        logger.debug(f"wait_for_{name}: Found {name} {id}")
        return True
    else:
        logger.error(
            f"wait_for_{name}: Could not find {name} {id} "
            "after being signalled. Should not get here."
        )
        return False


# Specialise for datasets and states
wait_for_dset = lambda id: wait_for_x(
    id, "dataset", lock_datasets, "datasets", waiting_datasets
)
wait_for_state = lambda id: wait_for_x(
    id, "state", lock_states, "states", waiting_states
)


@app.route("/update-datasets", methods=["POST"])
async def update_datasets(request):
    """Get an update on the datasets.

    Request all nodes that where added after the given timestamp.
    If the root of the given dataset is not among the given known roots,
    All datasets with the same root as the given dataset are included in the
    returned update additionally.

    This is called by kotekan's datasetManager.

    curl
    -d '{"ds_id":2143,"ts":0,"roots":[1,2,3]}'
    -X POST
    -H "Content-Type: application/json"
    http://localhost:12050/update-datasets
    """
    try:
        ds_id = request.json["ds_id"]
        ts = request.json["ts"]
        roots = request.json["roots"]
        logger.info("/update-datasets {} {} {}.".format(ds_id, ts, roots))

        reply = dict()
        reply["datasets"] = dict()

        # Do we know this ds ID?
        found = await wait_for_dset(ds_id)
        if not found:
            reply[
                "result"
            ] = "update-datasets: Dataset ID {} unknown to broker.".format(ds_id)
            logger.info("update-datasets: Dataset ID {} unknown.".format(ds_id))
            return response.json(reply)

        if ts is 0:
            ts = caput_time.datetime_to_unix(datetime.datetime.min)

        # If the requested dataset is from a tree not known to the calling
        # instance, send them that whole tree.
        ds = json.loads(await redis.execute("hget", "datasets", ds_id))
        root = await find_root(ds_id, ds)
        if root is None:
            logger.error("update-datasets: Root of dataset {} not found.".format(ds_id))
            reply["result"] = "Root of dataset {} not found.".format(ds_id)
        if root not in roots:
            reply["datasets"] = await tree(root)

        # add a timestamp to the result before gathering update
        reply["ts"] = caput_time.datetime_to_unix(datetime.datetime.utcnow())
        reply["datasets"].update(await gather_update(ts, roots))

        reply["result"] = "success"
        return response.json(reply)
    except Exception as e:
        logger.error("update-datasets: received exception %s", e)
        raise
    finally:
        logger.debug("update-datasets: finished")


@app.route("/internal-state", methods=["GET"])
async def internal_state(request):
    """Report on the internal state for debugging."""

    state = {
        "datasets_locked": lock_datasets.locked(),
        "states_locked": lock_states.locked(),
        "waiting_datasets": list(waiting_datasets.keys()),
        "waiting_states": list(waiting_states.keys()),
    }
    return response.json(state)


async def tree(root):
    """Return a list of all nodes in the given tree."""
    async with lock_datasets:
        datasets_of_root = json.loads(
            await redis.execute("hget", "datasets_of_root", root)
        )

        # Request all datasets concurrently
        # TODO: have redis fetch these all at once
        dsets = await asyncio.gather(
            *[redis.execute("hget", "datasets", n) for n in datasets_of_root]
        )
        tree = dict(zip(datasets_of_root, [json.loads(ds) for ds in dsets]))
    return tree


class Broker:
    """Main class to run the comet dataset broker."""

    # Todo: deprecated. the kwargs are only there to allow deprecated command line options
    def __init__(self, debug, port, timeout, **kwargs):
        self.config = {"debug": debug, "port": port}

        self.debug = debug
        self.startup_time = datetime.datetime.utcnow()
        self.port = None
        global wait_time
        wait_time = timeout

    @staticmethod
    def _flush_redis():
        """
        Flush from redis what we don't want to keep on start.

        At the moment this only deletes members of the set "requested_states".
        """
        r = redis_sync.Redis(REDIS_SERVER[0], REDIS_SERVER[1])
        hashes = r.hkeys("requested_states")
        for state_hash in hashes:
            logger.warning(
                "Found requested state in redis on startup: {}\nFlushing...".format(
                    state_hash.decode()
                )
            )
            if r.hdel("requested_states", state_hash) != 1:
                logger.error(
                    "Failure deleting {} from requested states in redis on startup.".format(
                        state_hash.decode()
                    )
                )
                exit(1)

    def _wait_and_register(self):

        # Wait until the port has been set (meaning comet is available)
        while not self.port:
            time.sleep(1)

        manager = Manager("localhost", self.port)
        try:
            manager.register_start(self.startup_time, __version__, self.config)
        except CometError as exc:
            logger.error(
                "Comet failed registering its own startup and initial config: {}".format(
                    exc
                )
            )
            exit(1)

    def run_comet(self):
        """Run comet dataset broker."""

        print(
            "Starting CoMeT dataset_broker {} using port {}.".format(
                __version__, self.config["port"]
            )
        )

        self._flush_redis()

        # # Register config with broker
        t = Thread(target=self._wait_and_register)
        t.start()

        # Check if port is set to 0 for random open port
        port = self.config["port"]
        server_kwargs = {}
        if port == 0:
            sock = socket()
            sock.bind(("0.0.0.0", 0))
            server_kwargs["sock"] = sock
            _, port = sock.getsockname()
            logger.info("Selected random port: {}".format(port))
        else:
            server_kwargs["host"] = "0.0.0.0"
            server_kwargs["port"] = port
        self.port = port

        app.run(
            workers=1,
            return_asyncio_server=True,
            log_config={},
            debug=False,
            **server_kwargs,
        )

        t.join()
        logger.info("Comet stopped.")


async def _create_locks(_, loop):
    global lock_states, lock_datasets, cond_states, cond_datasets
    lock_datasets = asyncio.Lock()
    lock_states = asyncio.Lock()
    cond_states = asyncio.Condition(lock_states)
    cond_datasets = asyncio.Condition(lock_datasets)


# Create the Redis connection pool, use sanic to start it so that it
# ends up in the same event loop
# At the same time create the locks that we will need
async def _init_redis_async(_, loop):
    logger.setLevel(logging.DEBUG)
    global redis
    redis = await aioredis.create_pool(
        REDIS_SERVER, encoding="utf-8", minsize=20, maxsize=50000
    )


async def _close_redis_async(_, loop):
    redis.close()
    await redis.wait_closed()


app.register_listener(_init_redis_async, "before_server_start")
app.register_listener(_create_locks, "before_server_start")
app.register_listener(_close_redis_async, "after_server_stop")
