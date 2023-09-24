"""
REST Server for CoMeT (the Dataset Broker).

Available endpoints:
- status
- get_states
- get_datasets
- register_state
- send_state
- register_dataset
- request-state
- update-datasets
- internal-states
"""
import aioredis
import asyncio
import contextvars
import datetime
import json
import ujson
import random
import logging
import redis as redis_sync
import time
import traceback
import os

from async_lru import alru_cache
from sanic import Sanic
from sanic import response
from sanic.log import logger
from socket import socket
from threading import Thread

from caput import time as caput_time

from . import __version__
from .manager import Manager, TIMESTAMP_FORMAT
from .exception import CometError, DatasetNotFoundError, StateNotFoundError

DEFAULT_PORT = 12050
REDIS_PORT = os.environ.get("REDIS_PORT", 6379)
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REQUESTED_STATE_TIMEOUT = 35
REDIS_SERVER = (REDIS_HOST, REDIS_PORT)

# config variable
wait_time = None

app = Sanic(__name__.replace(".", "-"))
app.config.REQUEST_TIMEOUT = 120
app.config.RESPONSE_TIMEOUT = 120

lock_datasets = None
lock_states = None

waiting_datasets = {}
waiting_states = {}

# will have a unique value for every request sent to the broker
# used for tracking sequences of events in the logs
request_id = contextvars.ContextVar("request_id", default=0)


def _json_dumps(*args, **kwargs):
    return ujson.dumps(*args, **kwargs, ensure_ascii=False, reject_bytes=False)


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
        logger.error(
            "status: threw exception {} while handling request from {}",
            str(e),
            request.ip,
        )
        traceback.print_exc()
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

        states = await redis.execute_command("hkeys", "states")
        reply = {"result": "success", "states": states}

        logger.debug("states: {}".format(states))
        return response.json(reply, dumps=_json_dumps)
    except Exception as e:
        logger.error(
            "states: threw exception {} while handling request from {}",
            str(e),
            request.ip,
        )
        traceback.print_exc()
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

        datasets = await redis.execute_command("hkeys", "datasets")
        reply = {"result": "success", "datasets": datasets}

        logger.debug("datasets: {}".format(datasets))
        return response.json(reply, dumps=_json_dumps)
    except Exception as e:
        logger.error(
            "datasets: threw exception {} while handling request from {}",
            str(e),
            request.ip,
        )
        traceback.print_exc()
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
    await redis.execute_command(
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
            try:
                await get_state(hash, wait=False)
            except StateNotFoundError:
                # we don't know this state, did we request it already?
                # After REQUEST_STATE_TIMEOUT we request it again.
                request_time = await redis.execute_command(
                    "hget", "requested_states", hash
                )
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
                await redis.execute_command(
                    "hset", "requested_states", hash, time.time()
                )
        return response.json(reply)
    except Exception as e:
        logger.error(
            "register-state: threw exception {} while handling request from {}",
            str(e),
            request.ip,
        )
        traceback.print_exc()
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
            try:
                found = await get_state(hash, wait=False)
            except StateNotFoundError:
                await redis.execute_command("hset", "states", hash, json.dumps(state))
                reply["result"] = "success"
                archive_state = True

                # Notify anything waiting for this state to arrive
                signal_created(hash, "state", lock_states, waiting_states)
            else:
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

        # Remove it from the set of requested states (if it's in there.)
        try:
            await asyncio.shield(
                redis.execute_command("hdel", "requested_states", hash)
            )
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
        logger.error(
            "send-state: threw exception {} while handling request from {}",
            str(e),
            request.ip,
        )
        traceback.print_exc()
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
            try:
                found = await get_dataset(hash, wait=False)
            except DatasetNotFoundError:
                if dataset_valid and root is not None:
                    # save the dataset
                    await redis.execute_command(
                        "hset", "datasets", hash, json.dumps(ds)
                    )

                    reply["result"] = "success"
                    archive_ds = True

                    # Notify anything waiting for this dataset to arrive
                    signal_created(hash, "dataset", lock_datasets, waiting_datasets)
            else:
                # if we know it already, does it differ?
                if found != ds:
                    reply["result"] = (
                        "error: hash collision ({})\nTrying to register the following dataset:\n{},\nbut a different one is know to "
                        "the broker with the same hash:\n{}".format(hash, ds, found)
                    )
                    logger.warning("register-dataset: {}".format(reply["result"]))
                else:
                    reply["result"] = "success"

        if archive_ds:
            await asyncio.shield(archive("dataset", request.json))

        # Done cleaning up, re-raise if this request got cancelled.
        if cancelled:
            raise cancelled

        return response.json(reply)
    except Exception as e:
        logger.error(
            "register-dataset: threw exception {} while handling request from {}",
            str(e),
            request.ip,
        )
        traceback.print_exc()
        raise
    finally:
        logger.debug("register-dataset: finished")


async def find_root(hash, ds):
    """Return the dataset Id of the root of this dataset."""
    while not ds["is_root"]:
        hash = ds["base_dset"]
        try:
            ds = await get_dataset(hash)
        except DatasetNotFoundError as err:
            logger.error("find_root: dataset {} not found: {}".format(hash, err))
            return None
    return hash


async def check_dataset(ds):
    """Check if a dataset is valid.

    For a dataset to be valid, the state and base dataset it references to
    have to exist. If it is a root dataset, the base dataset does not have
    to exist.
    """
    logger.debug("check_dataset: Checking dataset: {}".format(ds))
    try:
        await get_state(ds["state"])
    except StateNotFoundError as err:
        logger.debug("check_dataset: State of dataset {} unknown: {}".format(ds, err))
        return False
    if ds["is_root"]:
        logger.debug("check_dataset: dataset {} OK".format(ds))
        return True
    try:
        await get_dataset(ds["base_dset"])
    except DatasetNotFoundError as err:
        logger.debug(
            "check_dataset: Base dataset of dataset {} unknown: {}".format(ds, err)
        )
        return False

    logger.debug("check_dataset: dataset {} OK".format(ds))
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
        try:
            reply["state"] = await get_state(id)
        except StateNotFoundError as err:
            msg = "request-state: State {} unknown to broker: {}".format(id, err)
            reply["result"] = msg
            logger.info(msg)
            return response.json(reply)
        logger.debug("request-state: found state ID {}".format(id))

        reply["result"] = "success"
        logger.debug("request-state: Replying with state {}".format(id))
        return response.json(reply)
    except Exception as e:
        logger.error(
            "request-state: threw exception {} while handling request from {}",
            str(e),
            request.ip,
        )
        traceback.print_exc()
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
    if await redis.execute_command("hexists", redis_hash, id):
        return True

    logger.debug(f"wait_for_{name}: Waiting for {name} {id}")

    async with lock:
        # While we are locked, test again to ensure that we have the dataset
        if await redis.execute_command("hexists", redis_hash, id):
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

    if await redis.execute_command("hexists", redis_hash, id):
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


@alru_cache(maxsize=10000)
async def get_dataset(ds_id, wait=True):
    """
    Get a dataset by ID from redis (LRU cached).

    Parameters
    ----------
    ds_id : str
        Dataset ID
    wait : bool
        Wait before deciding that we don't have the dataset.

    Returns
    -------
    Dataset
        The dataset from cache or redis.

    Raises
    ------
    DatasetNotFoundError
        If the dataset doesn't exist or waiting for the dataset timed out.

    """
    if wait:
        # Check if existing and wait if not
        found = await wait_for_dset(ds_id)
        if not found:
            raise DatasetNotFoundError("Dataset {} not found: Timeout.".format(ds_id))

    # Get it from redis
    ds = await redis.execute_command("hget", "datasets", ds_id)
    if ds is None:
        raise DatasetNotFoundError("Dataset {} unknown to broker.".format(ds_id))
    return json.loads(ds)


@alru_cache(maxsize=1000)
async def get_state(state_id, wait=True):
    """
    Get a state by ID from redis (LRU cached).

    Parameters
    ----------
    state_id : str
        State ID
    wait : bool
        Wait before deciding that we don't have the state.

    Returns
    -------
    State
        The state from cache or redis.

    Raises
    ------
    StateNotFoundError
        If the state doesn't exist or waiting for the state timed out.
    """
    if wait:
        # Check if existing and wait if not
        found = await wait_for_state(state_id)
        if not found:
            raise StateNotFoundError("State {} not found: Timeout.".format(state_id))

    # Get it from redis
    state = await redis.execute_command("hget", "states", state_id)
    if state is None:
        raise StateNotFoundError("State {} unknown to broker.".format(state_id))
    return json.loads(state)


@app.route("/update-datasets", methods=["POST"])
async def update_datasets(request):
    """Get an update on the datasets.

    Returns all datasets between the given dataset and its root dataset.

    This is called by kotekan's datasetManager.

    curl
    -d '{"ds_id":2143,"ts":0,"roots":[1,2,3]}'
    -X POST
    -H "Content-Type: application/json"
    http://localhost:12050/update-datasets
    """
    start = time.time()
    try:
        ds_id = request.json["ds_id"]
        logger.info("/update-datasets {}.".format(ds_id))

        reply = dict()
        reply["datasets"] = dict()

        # Traverse up the tree and collect all datasets until the root
        while True:
            try:
                ds = await get_dataset(ds_id)
            except DatasetNotFoundError as err:
                msg = "update-datasets: {}.".format(err)
                reply["result"] = msg
                logger.info(msg)
                return response.json(reply)
            reply["datasets"][ds_id] = ds
            if ds["is_root"]:
                break
            ds_id = ds["base_dset"]

        reply["result"] = "success"
        return response.json(reply)
    except Exception as e:
        logger.error(
            "update-datasets: threw exception {} while handling request from {}",
            str(e),
            request.ip,
        )
        traceback.print_exc()
        raise
    finally:
        logger.debug("update-datasets: finished (took {}s)".format(time.time() - start))


@app.route("/internal-state", methods=["GET"])
async def internal_state(request):
    """Report on the internal state for debugging."""

    state = {
        "datasets_locked": lock_datasets.locked(),
        "states_locked": lock_states.locked(),
        "waiting_datasets": list(waiting_datasets.keys()),
        "waiting_states": list(waiting_states.keys()),
        "datasets_cache": get_dataset.cache_info(),
        "states_cache": get_state.cache_info(),
    }
    return response.json(state)


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
    url = "redis://{0}:{1}".format(*REDIS_SERVER)
    redis = aioredis.from_url(url, encoding="utf-8")


app.register_listener(_init_redis_async, "before_server_start")
app.register_listener(_create_locks, "before_server_start")
