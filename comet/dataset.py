"""Represents a dataset, linking a unique ID to a set of dataset states."""

from .hash import hash_dictionary


class Dataset:
    """A dataset, linking a dataset ID to a state and a base dataset.

    Parameters
    ----------
    state_id : str
        ID of the dataset state of this dataset.
    state_type : str
        Type of the dataset state of this dataset.
    dataset_id : str
        (optional) ID (hash) of this dataset. If not supplied, it's generated internally.
    base_dataset_id : str or None
        ID of the base dataset or `None` if this is a root dataset (default `None`).
    is_root : bool
        `True`, if this is a root dataset (default `False`).
    """

    def __init__(
        self, state_id, state_type, dataset_id=None, base_dataset_id=None, is_root=False
    ):
        if not is_root and base_dataset_id is None:
            raise ValueError(
                "A dataset needs to either have a base dataset or be a root dataset (found neither)."
            )
        self._state_id = state_id
        self._base_dataset_id = base_dataset_id
        self._state_type = state_type
        self._is_root = is_root
        if dataset_id is None:
            self._id = hash_dictionary(self.to_dict())
        else:
            self._id = dataset_id

    @classmethod
    def from_dict(cls, dict_, dataset_id=None):
        """Create a `Dataset` object from a dictionary.

        Parameters
        ----------
        dict_ : dict
            Dictionary containing `state : str` (the ID of the state), `base_dset : str` (ID of the
            base dataset, only if this is not a root dataset), `type : str` (type of the state),
            `is_root` (`True` if this is a root dataset, default: `False`).
        dataset_id : str
            (optional) ID (hash) of this dataset. If not supplied, it's generated internally.

        Returns
        -------
        State
            A state object.
        """
        if not isinstance(dict_, dict):
            raise ValueError(
                f"Expected parameter 'dict_' to be of type 'dict' (found {type(dict_).__name__})."
            )

        base_ds_id = dict_.get("base_dset", None)
        is_root = dict_.get("is_root", False)

        try:
            state_id = dict_["state"]
            state_type = dict_["type"]
        except KeyError as e:
            raise ValueError(
                f"Expected key '{e}' in state dict (found {dict_.keys()})."
            )

        return cls(state_id, state_type, dataset_id, base_ds_id, is_root)

    @property
    def id(self):
        """Get dataset ID.

        Returns
        -------
        str
            Dataset ID.
        """
        return self._id

    @property
    def state_id(self):
        """Get State ID.

        Returns
        -------
        str
            State ID.
        """
        return self._state_id

    @property
    def state_type(self):
        """Get type of state.

        Returns
        -------
        str
            State type.
        """
        return self._state_type

    @property
    def is_root(self):
        """Tell if this is a root dataset.

        Returns
        -------
        bool
            `True`, if this is a root dataset.
        """
        return self._is_root

    @property
    def base_dataset_id(self):
        """Get ID of base dataset.

        Returns
        -------
        str or None
            ID of the base dataset or `None` if this is a root dataset.
        """
        return self._base_dataset_id

    def to_dict(self):
        """Create dictionary from this Dataset object.

        Returns
        -------
        dict
            Dictionary than can be turned back into a Dataset object.
        """
        return {
            "is_root": self.is_root,
            "base_dset": self.base_dataset_id,
            "state": self.state_id,
            "type": self.state_type,
        }
