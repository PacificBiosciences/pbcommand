VERSION = (0, 1, 4)


def get_version():
    """Return the version as a string. "O.7"

    This uses a major.minor

    Each python module of the system (e.g, butler, detective, siv_butler.py)
    will use this version +  individual changelist. This allows top level
    versioning, and sub-component to be versioned based on a p4 changelist.

    .. note:: This should be improved to be compliant with PEP 386.
    """
    return ".".join([str(i) for i in VERSION])