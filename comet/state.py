"""Contains a state of a dataset."""

from copy import deepcopy

from .hash import hash_dictionary


class State:
    """A dataset state: links a state ID to some kind of metadata.

    Parameters
    ----------
    data : dict
        State data.
    state_type : str
        State type, e.g. "inputs", "metadata".
    state_id : str
        (optional) ID (hash) of this state. If not supplied, it's generated internally.
    """

    def __init__(self, data, state_type, state_id=None):
        self._data = deepcopy(data)
        self._type = state_type
        if state_id is None:
            self._id = hash_dictionary(self.to_dict())
        else:
            self._id = state_id

    @classmethod
    def from_dict(cls, dict_, state_id=None):
        """Create a `State` object from a dictionary.

        Parameters
        ----------
        dict_ : dict
            Dictionary with entry `type`. All additional entries make the state data.
        state_id : str
            (optional) ID (hash) of this state. If not supplied, it's generated internally.

        Returns
        -------
        State
            A `State` object.
        """
        if not isinstance(dict_, dict):
            raise ValueError(
                f"Expected parameter 'json_' to be of type 'dict' (found {type(dict_).__name__})."
            )

        try:
            state_type = dict_.pop("type")
        except KeyError:
            raise ValueError(
                f"Expected key 'type' in state json (found {dict_.keys()})."
            )

        return cls(dict_, state_type, state_id)

    @property
    def data(self):
        """Get state data.

        Returns
        -------
        dict
            State data.
        """
        return self._data

    @property
    def state_type(self):
        """Get state type.

        Returns
        -------
        str
            State type.
        """
        return self._type

    @property
    def id(self):
        """Get the ID of the state.

        Returns
        -------
        str
            State ID.
        """
        return self._id

    def to_dict(self):
        """Generate dictionary from state object.

        Returns
        -------
        dict
            Dictionary that can be parsed to a state object.
        """
        _dict = self.data
        _dict.update({"type": self.state_type})
        return _dict
