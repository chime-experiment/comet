"""REST Server for CoMeT (the Dataset Broker)."""
import asyncio
import datetime
from bisect import bisect_left
from signal import signal, SIGINT

from sanic import Sanic
from sanic.response import json
from sanic.log import logger

from . import __version__

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

    logger.debug('states: %r' % (states.keys()))
    logger.debug('datasets: %r' % (datasets.keys()))

    return json(reply)


@app.route('/register-state', methods=['POST'])
async def registerState(request):
    """Register a dataset state with CoMeT (the broker).

    This should only ever be called by kotekan's datasetManager.
    """
    global states
    global requested_states

    hash = request.json['hash']
    logger.debug('register-state: Received register state request, hash: {:x}'.format(hash))
    reply = dict(result="success")

    async with lock_states:
        if states.get(hash) is None:
            # we don't know this state, did we request it already?
            async with lock_requested_states:
                if hash in requested_states:
                    return json(reply)

            # ask for it
            async with lock_requested_states:
                requested_states.add(hash)
            reply['request'] = "get_state"
            reply['hash'] = hash
            logger.debug('register-state: Asking for state, hash: {:x}'.format(hash))
    return json(reply)


@app.route('/send-state', methods=['POST'])
async def sendState(request):
    """Send a dataset state to CoMeT (the broker).

    This should only ever be called by kotekan's datasetManager.
    """
    global states
    global requested_states

    hash = request.json['hash']
    state = request.json['state']
    logger.debug('send-state: Received state %r' % (hash))
    reply = dict()

    # do we have this state already?
    async with lock_states:
        found = states.get(hash)
        if found is not None:
            # if we know it already, does it differ?
            if found != state:
                reply['result'] = "error: a different state is know to " \
                                  "the broker with this hash: %r" % found
                logger.warning('send-state: Failure receiving state: a '
                               'different state with the same hash is: %r'
                               % (found))
            else:
                reply['result'] = "success"
        else:
            states[hash] = state
            reply['result'] = "success"
            signal_states_updated.notify_all()

    async with lock_requested_states:
        requested_states.remove(hash)

    return json(reply)


@app.route('/register-dataset', methods=['POST'])
async def registerDataset(request):
    """Register a dataset with CoMeT (the broker).

    This should only ever be called by kotekan's datasetManager.
    """
    global datasets

    hash = request.json['hash']
    ds = request.json['ds']
    logger.debug('register-dataset: Registering new dataset with hash %r : %r' % (hash, ds))
    dataset_valid = await checkDataset(ds)
    reply = dict()
    root = await findRoot(hash, ds)

    # dataset already known?
    async with lock_datasets:
        found = datasets.get(hash)
        if found is not None:
            # if we know it already, does it differ?
            if found != ds:
                reply['result'] = "error: a different dataset is know to" \
                                  " the broker with this hash: %r" % found
                logger.warning('register-dataset: Failure receiving dataset: a'
                               ' different dataset with the same hash is: %r' % (found))
            else:
                reply['result'] = "success"
        elif dataset_valid:
            # save the dataset
            saveDataset(hash, ds, root)

            reply['result'] = "success"
            signal_datasets_updated.notify_all()
        else:
            reply['result'] = 'Dataset {} invalid.'.format(hash)
            logger.debug(
                'register-dataset: Received invalid dataset with hash %r : %r' %
                (hash, ds))
        return json(reply)


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
            logger.error('findRoot: dataset %r not found.', hash)
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
    logger.debug('checkDataset: Checking dataset: %r' % (ds))
    found = await wait_for_state(ds['state'])
    if not found:
        logger.debug('checkDataset: State of dataset unknown: %r' % (ds))
        return False
    if ds['is_root']:
        logger.debug('checkDataset: dataset %r OK' % (ds))
        return True
    found = await wait_for_dset(ds['base_dset'])
    if not found:
        logger.debug('checkDataset: Base dataset of dataset unknown: %r' % (ds))
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

    logger.debug('request-state: Received request for state with ID %r' % (id))
    reply = dict()
    reply['id'] = id

    # Do we know this state ID?
    logger.debug('request-state: waiting for state ID %r' % (id))
    found = await wait_for_state(id)
    if not found:
        reply['result'] = "state ID %r unknown to broker." % id
        logger.info('request-state: State %r unknown to broker' % (id))
        return json(reply)
    logger.debug('request-state: found state ID %r' % (id))

    async with lock_states:
        reply['state'] = states[id]

    reply['result'] = "success"
    logger.debug('request-state: Replying with state %r' % (id))
    return json(reply)


async def wait_for_dset(id):
    """Wait until the given dataset is present."""
    global datasets

    found = True
    await lock_datasets.acquire()

    if datasets.get(id) is None:
        # wait for half of kotekans timeout before we admit we don't have it
        lock_datasets.release()
        logger.debug('wait_for_ds: Waiting for dataset %r' % (id))
        while True:
            # did someone send it to us by now?
            async with lock_datasets:
                try:
                    await asyncio.wait_for(signal_datasets_updated.wait(), WAIT_TIME)
                except TimeoutError:
                    logger.warning('wait_for_ds: Timeout (%rs) when waiting for dataset %r'
                                   % (WAIT_TIME, id))
                    return False
                if datasets.get(id) is not None:
                    logger.debug(
                        'wait_for_ds: Found dataset %r' % (id))
                    break

        await lock_datasets.acquire()
        try:
            if datasets.get(id) is None:
                logger.warning('wait_for_ds: Timeout (%rs) when waiting for dataset %r'
                               % (WAIT_TIME, id))
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
        logger.debug('wait_for_state: Waiting for state %r' % (id))
        while True:
            # did someone send it to us by now?
            async with lock_states:
                try:
                    await asyncio.wait_for(signal_states_updated.wait(), WAIT_TIME)
                except TimeoutError:
                    logger.warning('wait_for_ds: Timeout (%rs) when waiting for state %r'
                                   % (WAIT_TIME, id))
                    return False
                if states.get(id) is not None:
                    logger.debug(
                        'wait_for_ds: Found state %r' % (id))
                    break

        await lock_states.acquire()
        try:
            if states.get(id) is None:
                logger.warning('wait_for_state: Timeout (%rs) when waiting for state %r'
                               % (WAIT_TIME, id))
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

    logger.debug('update-datasets: Received request for ancestors of dataset %r '
                 'since timestamp %r, roots %r.' % (ds_id, ts, roots))
    reply = dict()
    reply['datasets'] = dict()

    # Do we know this ds ID?
    found = await wait_for_dset(ds_id)
    if not found:
        reply['result'] = "update-datasets: Dataset ID %r unknown to broker." % ds_id
        logger.info('update-datasets: Dataset ID %r unknown.' % (ds_id))
        return json(reply)

    if ts is 0:
        ts = datetime_to_float(datetime.datetime.min)

    # If the requested dataset is from a tree not known to the calling
    # instance, send them that whole tree.
    root = await findRoot(ds_id, datasets[ds_id])
    if root is None:
        logger.error('update-datasets: Root of dataset %r not found.', ds_id)
        reply['result'] = 'Root of dataset %r not found.' % ds_id
    if root not in roots:
        reply['datasets'] = await tree(root)

    # add a timestamp to the result before gathering update
    reply['ts'] = datetime_to_float(datetime.datetime.utcnow())
    reply['datasets'].update(await gatherUpdate(ts, roots))

    reply['result'] = "success"
    logger.debug('update-datasets: Answering with %r.' % (reply))
    return json(reply)


async def tree(root):
    """Return a list of all nodes in the given tree."""
    tree = dict()
    async with lock_datasets:
        for n in datasets_of_root[root]:
            tree[n] = datasets[n]
    return tree


class Broker():
    """Main class to run the comet dataset broker."""

    def __init__(self, debug):
        self.debug = debug

    def run(self):
        """Run comet dataset broker."""
        print("Starting CoMeT dataset_broker({}) using port {}."
              .format(__version__, DEFAULT_PORT))
        server = app.create_server(host="0.0.0.0", port=12050, return_asyncio_server=True,
                                   access_log=True, debug=self.debug)
        loop = asyncio.get_event_loop()
        task = asyncio.ensure_future(server)
        signal(SIGINT, lambda s, f: loop.stop())
        try:
            loop.run_forever()
        except BaseException:
            loop.stop()
            raise
