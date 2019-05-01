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
        self.config = None
        self.states = dict()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

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
            return self.states[self.config]

        raise ManagerError('get_state: Not implemented for anything but initial config.')
