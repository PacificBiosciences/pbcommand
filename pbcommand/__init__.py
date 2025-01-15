from importlib.metadata import Distribution, PackageNotFoundError
import sys

try:
   __VERSION__ = Distribution.from_name('pbcommand').version
except PackageNotFoundError:
    __VERSION__ = 'unknown'

VERSION = (int(x) for x in __VERSION__.split('.'))


def get_version():
    """Return the version as a string. "1.0.0"

    This uses a major.minor.tiny to be compatible with semver spec.

    .. note:: This should be improved to be compliant with PEP 386.
    """
    return ".".join([str(i) for i in VERSION])


def to_ascii(s):
    return s


def to_utf8(s):
    return s
