"""Contains a state of a dataset."""

from .hash import hash_dictionary

from copy import deepcopy


class State:
    """
    A dataset state: links a state ID to some kind of metadata.

    Parameters
    ----------
    data : dict
        State data.
    state_type : str
        State type, e.g. "inputs", "metadata".
    """

    def __init__(self, data, state_type):
        self._data = deepcopy(data)
        self._type = state_type
        self._id = hash_dictionary(self.to_dict())

    @classmethod
    def from_dict(cls, dict_):
        """
        Create a `State` object from a dictionary.

        Parameters
        ----------
        dict_ : dict
            Dictionary with an entry `type`. All additional entries make the state data.

        Returns
        -------
        State
            A `State` object.
        """
        if not isinstance(dict_, dict):
            raise ValueError(
                "Expected parameter 'json_' to be of type 'dict' (found {}).".format(
                    type(dict_).__name__
                )
            )

        try:
            state_type = dict_.pop("type")
        except KeyError:
            raise ValueError(
                "Expected key 'type' in state json (found {}).".format(dict_.keys())
            )

        return cls(dict_, state_type)

    @property
    def data(self):
        """
        Get state data.

        Returns
        -------
        dict
            State data.
        """
        return self._data

    @property
    def state_type(self):
        """
        Get state type.

        Returns
        -------
        str
            State type.
        """
        return self._type

    @property
    def id(self):
        """
        Get the ID of the state.

        Returns
        -------
        str
            State ID.
        """
        return self._id

    def to_dict(self):
        """
        Generate dictionary from state object.

        Returns
        -------
        dict
            Dictionary that can be parsed to a state object.
        """
        _dict = self.data
        _dict.update({"type": self.state_type})
        return _dict
