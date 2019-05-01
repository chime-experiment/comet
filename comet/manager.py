"""CoMeT dataset manager."""

import inspect
import logging
import requests
import json

# Endpoint names:
REGISTER_STATE = '/register-state'
REGISTER_DATASET = '/register-dataset'
SEND_STATE = '/send-state'
REGISTER_EXTERNAL_STATE = '/register-external-state'

INITIAL_CONFIG_TYPE = 'initial_config_registered_with_comet'


class ManagerError(BaseException):
    """There was an internal error in dataset management."""

    pass


class BrokerError(BaseException):
    """There was an error registering states or datasets with the broker."""

    pass


class Tree(object):
    """
    Dataset tree node.

    Used to keep track of the dataset hirarchy.
    """

    def __init__(self, dataset, parent=None):
        self.data = dataset
        self.children = []
        self.parent = parent

    def _add_child(self, node):
        """
        Add a child to this node.

        Parameters
        ----------
        node : :class:`Tree`
        """
        assert isinstance(node, Tree)
        self.children.append(node)
        node.parent = self


class Manager:
    """
    CoMeT dataset manager.

    An interface for the CoMeT dataset broker. State changes are passed to the manager,
    it takes care of registering everything with the broker and keeps local copies to reduce
    network traffic.
    """

    def __init__(self, host, port, noconfig=False):
        """Set up the CoMeT dataset manager.

        Parameters
        ----------
        host : str
            Dataset broker host.
        port : int
            Dataset broker port.
        noconfig : bool
            If this is `True`, the manager doesn't check if the config was registered already, when
            a state is registered. Only use this if you know what you are doing. Default: `False`.
        """
        self.broker = 'http://{}:{}'.format(host, port)
        self.noconfig = noconfig
        self.root_dataset = None
        self.config = None
        self.states = dict()
        self.datasets = dict()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.last_state = dict()
        self.simple = True
        self.last_simple = None

    def register_config(self, config):
        """Register a static config with the broker.

        This should only be called once. If you want to register a state that may change, use
        :function:`register_state` instead.

        This does not attach the state to a dataset. If that's what you want to do, use
        :function:`register_state` instead.

        Parameters
        ----------
        config : dict
            The config should be JSON-serializable, preferably a dictionary.

        Raises
        ------
        :class:`ManagerError`
            If there was an internal error in the dataset management. E.g. if this was called
            already before. This is to register a static config. Once.
        :class:`BrokerError`
            If there was an error in registering stuff with the broker.
        :class:`ConnectionError`
            If the broker can't be reached.

        """
        if self.noconfig:
            raise ManagerError('Option `noconfig` was set but a config registered.')
        if config is None:
            raise ManagerError('The config can not be `None`.')
        if self.config:
            raise ManagerError('A config was already registered, this can only be done once.')

        state_id = self._make_hash(config)

        # get name of callers module
        name = inspect.getmodule(inspect.stack()[1][0]).__name__
        self.logger.info('Registering config for {}.'.format(name))
        request = {'hash': state_id, 'type': '{}_{}'.format(name, INITIAL_CONFIG_TYPE)}
        r = requests.post(self.broker + REGISTER_EXTERNAL_STATE, data=json.dumps(request))
        r.raise_for_status()
        r = r.json()
        self._check_result(r.get('result'), REGISTER_STATE)

        # Does the broker ask for the state?
        if r.get('request') == 'get_state':
            if r.get('hash') != state_id:
                raise BrokerError('The broker is asking for state {} when state {} (config) was '
                                  'registered.'.format(r.get('hash'), state_id))
            self._send_state(state_id, config)

        self.states[state_id] = config
        self.config = state_id

        return

    def _send_state(self, state_id, state):
        self.logger.debug('sending state {}'.format(state_id))
        request = {'hash': state_id, 'state': state}
        r = requests.post(self.broker + SEND_STATE, data=json.dumps(request))
        r.raise_for_status()
        r = r.json()
        self._check_result(r.get('result'), SEND_STATE)

    @staticmethod
    def _check_result(result, endpoint):
        if result != 'success':
            raise BrokerError('The broker answered with result `{}` to `{}`, expected `success`.'
                              .format(result, endpoint))

    @staticmethod
    def _make_hash(data):
        return hash(json.dumps(data, sort_keys=True))

    def register_state(self, state_type, state, parent_dataset=None):
        """Register a new state with the broker.

        If the state changes, you should call this function again, passing the same type and the
        new state. For static configs that don't change, use :function:`register_config`.

        If you have a single-lined dataset hirarchy, ignore the `parent_dataset` option. If you
        you have differing states that come from the same parent dataset, sharing a common state,
        you have to keep track of the dataset IDs.

        Parameters
        ----------
        state_type : str
            The name of the type of state to register.
        state : dict
            The state to register.
        parent_dataset: int
            If this is not `None`, this is the parent dataset, the new dataset with the given state
            is based on. Default: `None`.

        Raises
        ------
        :class:`ManagerError`
            If there was an internal error in dataset management. For example if a state was
            registered before the static config without initializing the manager with
            `noconfig=True`.
        :class:`BrokerError`
            If there was an error in registering stuff with the broker.
        :class:`ConnectionError`
            If the broker can't be reached.
        """
        if state is None:
            raise ManagerError('The config can not be `None`.')
        if self.root_dataset is None and self.noconfig is False:
            raise ManagerError('Config needs to be registered before any state is added.')
        if parent_dataset:
            self.simple = False
            try:
                parent_tree_node = self.datasets[parent_dataset]
            except KeyError:
                raise ManagerError('parent_dataset {} unknown.'.format(parent_dataset))
            if parent_tree_node is None:
                raise ManagerError('Unknown parent_dataset of state_type `{}`: {}'
                                   .format(state_type, parent_dataset))
        if not self.simple and parent_dataset is None:
            raise ManagerError('No parent_dataset given, but the dataset topology is not simple.')
        state_id = self._make_hash(state)

        self.logger.info('Registering {} state with hash {}.'.format(state_type, state_id))
        request = {'hash': state_id}
        r = requests.post(self.broker + REGISTER_STATE, data=json.dumps(request))
        r.raise_for_status()
        r = r.json()
        self._check_result(r.get('result'), REGISTER_STATE)

        # Does the broker ask for the state?
        if r.get('request') == 'get_state':
            if r.get('hash') != state_id:
                raise BrokerError('The broker is asking for state {} when state {} was registered.'
                                  .format(r.get('hash'), state_id))
            self._send_state(state_id, state)

        self.states[state_id] = state
        self.last_state[state_type] = state

        # Is this a root dataset?
        if self.noconfig and not self.datasets:
            self.logger.debug('Registering {} state {} with the root dataset.'
                              .format(state_type, state_id))
            is_root = True
        else:
            is_root = False

            # Find parent dataset
            if parent_dataset is None:
                parent_tree_node = self.last_simple

        ds = {'state': state_id, 'is_root': is_root, 'base_dset': parent_tree_node.data['hash']}
        ds_id = self._make_hash(ds)
        request = {'hash': ds_id, 'ds': ds}
        r = requests.post(self.broker + REGISTER_DATASET, data=json.dumps(request))
        r.raise_for_status()
        self._check_result(r.json().get('result'), REGISTER_DATASET)

        ds['hash'] = ds_id
        ds['type'] = state_type
        new_tree_node = Tree(ds)
        if is_root:
            self.root_dataset = new_tree_node
        else:
            parent_tree_node._add_child(new_tree_node)
        self.last_simple = new_tree_node
        self.datasets[ds_id] = new_tree_node
        return ds_id

    def get_state(self, type=None, dataset_id=None):
        """
        Get the static config or the dataset state that was added last.

        If called without parameters, this returns the static config that was registered.

        Note
        ----
        This only finds state that where registered locally with the comet manager.

        TODO
        ----
        Ask the broker for unknown states.

        Parameters
        ----------
        type : str
            The name of the type of the state to return. If this is `None`, the initial config will
            be returned. Default: `None`.
        dataset_id : int
            The ID of the dataset the last state of given type should be returned for. If this is
            `None` the dataset hirarchy is assumed to not have multiple branches. Default: `None`.

        Returns
        -------
        dict
            The last config or state of requested type that was registered. Returns `None` if
            requested state not found.
        """
        if type is None:
            return self.states[self.root_dataset.data['state']]

        if dataset_id is None:
            if not self.simple:
                raise ManagerError('No dataset_id given, when the dataset hirarchy has multiple'
                                   ' branches.')
            return self.last_state[type]

        # Walk up the tree to find the last dataset with the state of requested type.
        try:
            node = self.datasets[dataset_id]
        except KeyError:
            raise ManagerError('Dataset {} unknown.'.format(dataset_id))
        while node.data['type'] != type:
            if node.parent is None:
                return None
            node = node.parent
        return self.states[node.data['state']]
