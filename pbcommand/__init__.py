import sys

VERSION = (1, 8, 6)


def get_version():
    """Return the version as a string. "1.0.0"

    This uses a major.minor.tiny to be compatible with semver spec.

    .. note:: This should be improved to be compliant with PEP 386.
    """
    return ".".join([str(i) for i in VERSION])


python2 = (sys.version_info[0] < 3)


def to_ascii(s):
    if python2:
        return s.encode('ascii', 'ignore')
    else:
        return s


def to_utf8(s):
    if python2:
        return s.encode('utf8')
    else:
        return s


# https://docs.python.org/3/howto/sorting.html
def cmp_to_key(mycmp):
    'Convert a cmp= function into a key= function'
    class K:
        def __init__(self, obj, *args):
            self.obj = obj

        def __lt__(self, other):
            return mycmp(self.obj, other.obj) < 0

        def __gt__(self, other):
            return mycmp(self.obj, other.obj) > 0

        def __eq__(self, other):
            return mycmp(self.obj, other.obj) == 0

        def __le__(self, other):
            return mycmp(self.obj, other.obj) <= 0

        def __ge__(self, other):
            return mycmp(self.obj, other.obj) >= 0

        def __ne__(self, other):
            return mycmp(self.obj, other.obj) != 0
    return K
