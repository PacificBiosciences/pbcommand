VERSION = (1, 1, 1)


def get_version():
    """Return the version as a string. "1.0.0"

    This uses a major.minor.tiny to be compatible with semver spec.

    .. note:: This should be improved to be compliant with PEP 386.
    """
    return ".".join([str(i) for i in VERSION])
