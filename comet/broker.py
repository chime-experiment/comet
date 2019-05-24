"""REST Server for CoMeT (the Dataset Broker)."""
import asyncio
import datetime
from bisect import bisect_left
from copy import copy
from signal import signal, SIGINT
from threading import Thread
from time import sleep

from sanic import Sanic
from sanic import response
from sanic.log import logger
from concurrent.futures import CancelledError

from . import __version__
from .dumper import Dumper
from .manager import Manager, CometError

WAIT_TIME = 40
DEFAULT_PORT = 12050

app = Sanic(__name__)


def datetime_to_float(d):
    """
    Convert datetime to float (seconds).

    Parameters
    ----------
    d : datetime
        :class:`datetime` object to convert.

    Returns
    -------
    float
        The datetime in seconds.
    """
    epoch = datetime.datetime.utcfromtimestamp(0)
    total_seconds = (d - epoch).total_seconds()
    # total_seconds will be in decimals (millisecond precision)
    return total_seconds


def float_to_datetime(fl):
    """
    Convert a float (seconds) to datetime.

    Parameters
    ----------
    fl : float
        Seconds to convert.

    Returns
    -------
    :class:`datetime`
        The seconds converted to datetime.
    """
    return datetime.datetime.utcfromtimestamp(fl)


# Dumps all requests and states to file.
dumper = None

# Global state variables
states = dict()

# Hashmap of datasets for direct access.
datasets = dict()

# Two dicts of root -> list of (ds_id/timestamp) / timesamp. The latter
# facilitates sorted insert into the first.
datasets_of_root = dict()
datasets_of_root_keys = dict()

lock_datasets = asyncio.Lock()
lock_states = asyncio.Lock()
signal_states_updated = asyncio.Condition(lock_states)
signal_datasets_updated = asyncio.Condition(lock_datasets)

# a set of requested states and its lock
requested_states = set()
lock_requested_states = asyncio.Lock()

# external state:
# This is a state that consists of smaller external states that are not attached to any dataset.
external_state = dict()
lock_external_state = asyncio.Lock()


@app.route('/status', methods=['GET'])
async def status(request):
    """
    Get status of CoMeT (dataset-broker).

    Shows all datasets and states registered by CoMeT (the broker).

    curl -X GET http://localhost:12050/status
    """
    global states
    global datasets

    logger.debug('status: Received status request')

    reply = dict()
    async with lock_states:
        reply["states"] = states.keys()
    async with lock_datasets:
        reply["datasets"] = datasets.keys()

    logger.debug('states: {}'.format(states.keys()))
    logger.debug('datasets: {}'.format(datasets.keys()))

    return response.json(reply)


@app.route('/register-external-state', methods=['POST'])
async def externalState(request):
    """Register an external state that should is detached from any dataset."""
    global external_state

    hash = request.json['hash']
    type = request.json['type']
    logger.debug('Received external state: {} with hash {}'.format(type, hash))

    result = await registerState(request)

    state = {type: hash}
    async with lock_external_state:
        external_state.update(state)
        if request.json.get("dump", True):
            ext_state_dump = {'external-state': copy(external_state)}

    if request.json.get("dump", True):
        if 'time' in request.json:
            ext_state_dump['time'] = request.json['time']
        await dumper.dump(ext_state_dump)

    # TODO: tell kotekan that this happened

    return result


@app.route('/register-state', methods=['POST'])
async def registerState(request):
    """Register a dataset state with CoMeT (the broker).

    This should only ever be called by kotekan's datasetManager.
    """
    global states
    global requested_states

    hash = request.json['hash']
    logger.debug('register-state: Received register state request, hash: {}'.format(hash))
    reply = dict(result="success")

    async with lock_states:
        state = states.get(hash)
        if state is None:
            # we don't know this state, did we request it already?
            async with lock_requested_states:
                if hash in requested_states:
                    return response.json(reply)

            # ask for it
            async with lock_requested_states:
                requested_states.add(hash)
            reply['request'] = "get_state"
            reply['hash'] = hash
            logger.debug('register-state: Asking for state, hash: {}'.format(hash))

    if request.json.get("dump", True):
        if state is not None:
            # Dump state to file
            state_dump = {'state': state, 'hash': hash}
            await dumper.dump(state_dump)

    return response.json(reply)


@app.route('/send-state', methods=['POST'])
async def sendState(request):
    """Send a dataset state to CoMeT (the broker).

    This should only ever be called by kotekan's datasetManager.
    """
    global states
    global requested_states
    hash = request.json['hash']
    state = request.json['state']
    type = state['type']
    logger.debug('send-state: Received {} state {}'.format(type, hash))
    reply = dict()

    # do we have this state already?
    async with lock_states:
        found = states.get(hash)
        if found is not None:
            # if we know it already, does it differ?
            if found != state:
                reply['result'] = "error: a different state is know to " \
                                  "the broker with this hash: {}".format(found)
                logger.warning('send-state: Failure receiving state: a different state with the '
                               'same hash is: {}'.format(found))
            else:
                reply['result'] = "success"
        else:
            states[hash] = state
            reply['result'] = "success"
            signal_states_updated.notify_all()

    async with lock_requested_states:
        requested_states.remove(hash)

    if request.json.get("dump", True):
        # Dump state to file
        state_dump = {'state': state, 'hash': hash}
        await dumper.dump(state_dump)

    return response.json(reply)


@app.route('/register-dataset', methods=['POST'])
async def registerDataset(request):
    """Register a dataset with CoMeT (the broker).

    This should only ever be called by kotekan's datasetManager.
    """
    global datasets

    hash = request.json['hash']
    ds = request.json['ds']
    logger.debug('register-dataset: Registering new dataset with hash {} : {}'.format(hash, ds))
    dataset_valid = await checkDataset(ds)
    reply = dict()
    root = await findRoot(hash, ds)

    # Only dump new datasets.
    dump = False

    # dataset already known?
    async with lock_datasets:
        found = datasets.get(hash)
        if found is not None:
            # if we know it already, does it differ?
            if found != ds:
                reply['result'] = "error: a different dataset is know to" \
                                  " the broker with this hash: {}".format(found)
                logger.warning('register-dataset: Failure receiving dataset: a different dataset'
                               ' with the same hash is: {}'.format(found))
            else:
                reply['result'] = "success"
        elif dataset_valid:
            # save the dataset
            saveDataset(hash, ds, root)
            dump = request.json.get("dump", True)

            reply['result'] = "success"
            signal_datasets_updated.notify_all()
        else:
            reply['result'] = 'Dataset {} invalid.'.format(hash)
            logger.debug('register-dataset: Received invalid dataset with hash {} : {}'
                         .format(hash, ds))

    # Dump dataset to file
    if dump:
        ds_dump = {'ds': ds, 'hash': hash}
        if 'time' in request.json:
            ds_dump['time'] = request.json['time']
        await dumper.dump(ds_dump)

    return response.json(reply)


def saveDataset(hash, ds, root):
    """Save the given dataset, its hash and a current timestamp.

    This should be called while a lock on the datasets is held.
    """
    # add a timestamp to the dataset (ms precision)
    ts = datetime_to_float(datetime.datetime.utcnow())

    # create entry if this is the first node with that root
    if root not in datasets_of_root.keys():
        datasets_of_root[root] = list()
        datasets_of_root_keys[root] = list()

    # Determine where to insert dataset ID.
    i = bisect_left(datasets_of_root_keys[root], ts)

    # Insert timestamp in keys list.
    datasets_of_root_keys[root].insert(i, ts)

    # Insert the dataset ID itself in the corresponding place.
    datasets_of_root[root].insert(i, hash)

    # Insert the dataset in the hashmap
    datasets[hash] = ds


async def gatherUpdate(ts, roots):
    """Gather the update for a given time and roots.

    Returns a dict of dataset ID -> dataset with all datasets with the
    given roots that were registered after the given timestamp.
    """
    update = dict()

    async with lock_datasets:
        for r in roots:
            tree = reversed(datasets_of_root[r])
            keys = reversed(datasets_of_root_keys[r])

            # The nodes in tree are ordered by their timestamp from new to
            # old, so we are done as soon as we find an older timestamp than
            # the given one.
            for n, k in zip(tree, keys):
                if k < ts:
                    break
                update[n] = datasets[n]
    return update


async def findRoot(hash, ds):
    """Return the dataset Id of the root of this dataset."""
    root = hash
    while not ds['is_root']:
        root = ds['base_dset']
        found = await wait_for_dset(root)
        if not found:
            logger.error('findRoot: dataset {} not found.'.format(hash))
            return None
        async with lock_datasets:
            ds = datasets[root]
    return root


async def checkDataset(ds):
    """Check if a dataset is valid.

    For a dataset to be valid, the state and base dataset it references to
    have to exist. If it is a root dataset, the base dataset does not have
    to exist.
    """
    logger.debug('checkDataset: Checking dataset: {}'.format(ds))
    found = await wait_for_state(ds['state'])
    if not found:
        logger.debug('checkDataset: State of dataset unknown: {}'.format(ds))
        return False
    if ds['is_root']:
        logger.debug('checkDataset: dataset {} OK'.format(ds))
        return True
    found = await wait_for_dset(ds['base_dset'])
    if not found:
        logger.debug('checkDataset: Base dataset of dataset unknown: {}'.format(ds))
        return False
    return True


@app.route('/request-state', methods=['POST'])
async def requestState(request):
    """Request the state with the given ID.

    This is called by kotekan's datasetManager.

    curl -d '{"state_id":42}' -X POST -H "Content-Type: application/json"
         http://localhost:12050/request-state
    """
    global states
    id = request.json['id']

    logger.debug('request-state: Received request for state with ID {}'.format(id))
    reply = dict()
    reply['id'] = id

    # Do we know this state ID?
    logger.debug('request-state: waiting for state ID {}'.format(id))
    found = await wait_for_state(id)
    if not found:
        reply['result'] = "state ID {} unknown to broker.".format(id)
        logger.info('request-state: State {} unknown to broker'.format(id))
        return response.json(reply)
    logger.debug('request-state: found state ID {}'.format(id))

    async with lock_states:
        reply['state'] = states[id]

    reply['result'] = "success"
    logger.debug('request-state: Replying with state {}'.format(id))
    return response.json(reply)


async def wait_for_dset(id):
    """Wait until the given dataset is present."""
    global datasets

    found = True
    await lock_datasets.acquire()

    if datasets.get(id) is None:
        # wait for half of kotekans timeout before we admit we don't have it
        lock_datasets.release()
        logger.debug('wait_for_ds: Waiting for dataset {}'.format(id))
        while True:
            # did someone send it to us by now?
            async with lock_datasets:
                try:
                    await asyncio.wait_for(signal_datasets_updated.wait(), WAIT_TIME)
                except (TimeoutError, CancelledError):
                    logger.warning('wait_for_ds: Timeout ({}s) when waiting for dataset {}'
                                   .format(WAIT_TIME, id))
                    return False
                if datasets.get(id) is not None:
                    logger.debug('wait_for_ds: Found dataset {}'.format(id))
                    break

        await lock_datasets.acquire()
        try:
            if datasets.get(id) is None:
                logger.warning('wait_for_ds: Timeout ({}s) when waiting for dataset {}'
                               .format(WAIT_TIME, id))
                found = False
        finally:
            lock_datasets.release()
    else:
        lock_datasets.release()

    return found


async def wait_for_state(id):
    """Wait until the given state is present."""
    global states

    found = True
    await lock_states.acquire()
    if states.get(id) is None:
        # wait for half of kotekans timeout before we admit we don't have it
        lock_states.release()
        logger.debug('wait_for_state: Waiting for state {}'.format(id))
        while True:
            # did someone send it to us by now?
            async with lock_states:
                try:
                    await asyncio.wait_for(signal_states_updated.wait(), WAIT_TIME)
                except (TimeoutError, CancelledError):
                    logger.warning('wait_for_ds: Timeout ({}s) when waiting for state {}'
                                   .format(WAIT_TIME, id))
                    return False
                if states.get(id) is not None:
                    logger.debug('wait_for_ds: Found state {}'.format(id))
                    break

        await lock_states.acquire()
        try:
            if states.get(id) is None:
                logger.warning('wait_for_state: Timeout ({}s) when waiting for state {}'
                               .format(WAIT_TIME, id))
                found = False
        finally:
            lock_states.release()
    else:
        lock_states.release()

    return found


@app.route('/update-datasets', methods=['POST'])
async def updateDatasets(request):
    """Get an update on the datasets.

    Request all nodes that where added after the given timestamp.
    If the root of the given dataset is not among the given known roots,
    All datasets with the same root as the given dataset are included in the
    returned update additionally.

    This is called by kotekan's datasetManager.

    curl
    -d '{"ds_id":2143,"ts":0}'
    -X POST
    -H "Content-Type: application/json"
    http://localhost:12050/update-datasets
    """
    global datasets
    ds_id = request.json['ds_id']
    ts = request.json['ts']
    roots = request.json['roots']

    logger.debug('update-datasets: Received request for ancestors of dataset {} since timestamp '
                 '{}, roots {}.'.format(ds_id, ts, roots))
    reply = dict()
    reply['datasets'] = dict()

    # Do we know this ds ID?
    found = await wait_for_dset(ds_id)
    if not found:
        reply['result'] = "update-datasets: Dataset ID {} unknown to broker.".format(ds_id)
        logger.info('update-datasets: Dataset ID {} unknown.'.format(ds_id))
        return response.json(reply)

    if ts is 0:
        ts = datetime_to_float(datetime.datetime.min)

    # If the requested dataset is from a tree not known to the calling
    # instance, send them that whole tree.
    root = await findRoot(ds_id, datasets[ds_id])
    if root is None:
        logger.error('update-datasets: Root of dataset {} not found.'.format(ds_id))
        reply['result'] = 'Root of dataset {} not found.'.format(ds_id)
    if root not in roots:
        reply['datasets'] = await tree(root)

    # add a timestamp to the result before gathering update
    reply['ts'] = datetime_to_float(datetime.datetime.utcnow())
    reply['datasets'].update(await gatherUpdate(ts, roots))

    reply['result'] = "success"
    logger.debug('update-datasets: Answering with {}.'.format(reply))
    return response.json(reply)


async def tree(root):
    """Return a list of all nodes in the given tree."""
    tree = dict()
    async with lock_datasets:
        for n in datasets_of_root[root]:
            tree[n] = datasets[n]
    return tree


class Broker():
    """Main class to run the comet dataset broker."""

    def __init__(self, data_dump_path, file_lock_time, debug):
        global dumper

        self.config = {"data_dump_path": data_dump_path, "file_lock_time": file_lock_time,
                       "debug": debug}

        dumper = Dumper(data_dump_path, file_lock_time)
        self.debug = debug
        self.startup_time = datetime.datetime.utcnow()

    @staticmethod
    def _wait_and_register(startup_time, config):
        sleep(1)
        manager = Manager("localhost", DEFAULT_PORT)
        try:
            manager.register_start(startup_time, __version__)
            manager.register_config(config)
        except CometError as exc:
            logger.error('Comet failed registering its own startup and initial config: {}'
                         .format(exc))
            del dumper
            exit(1)

    def run(self):
        """Run comet dataset broker."""
        global dumper

        print("Starting CoMeT dataset_broker({}) using port {}."
              .format(__version__, DEFAULT_PORT))
        server = app.create_server(host="0.0.0.0", port=DEFAULT_PORT, return_asyncio_server=True,
                                   access_log=True, debug=self.debug)
        loop = asyncio.get_event_loop()
        task = asyncio.ensure_future(server)
        signal(SIGINT, lambda s, f: loop.stop())

        # Register config with broker
        t = Thread(target=self._wait_and_register, args=(self.startup_time, self.config,))
        t.start()

        try:
            loop.run_forever()
        except BaseException:
            loop.stop()
            del dumper
            raise
        del dumper
