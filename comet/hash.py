"""Hashing utilities used by comet."""

import json
import mmh3


def hash_dictionary(dict_):
    """
    Create 128bit MurmurHash3 of a dictionary.

    Parameters
    ----------
    dict_ : dict
        Dictionary to hash.

    Returns
    -------
    str
        Hash of dictionary json-serialization.
    """
    return "%032x" % mmh3.hash128(json.dumps(dict_, sort_keys=True), seed=1420)
