import os
import tempfile

HAS_PBCORE = False

try:
    import pbcore
    HAS_PBCORE = True
except ImportError:
    HAS_PBCORE = False


def pbcore_skip_msg(msg=None):
    msg = "" if msg is None else msg
    return "" if HAS_PBCORE else "pbcore is not installed. {m}".format(m=msg)


def get_temp_file(suffix, dir_):
    t = tempfile.NamedTemporaryFile(suffix=suffix, delete=False, dir=dir_)
    t.close()
    return t.name


def get_temp_dir(suffix=""):
    """This will make subdir in the root tmp dir"""
    return tempfile.mkdtemp(dir=None, suffix=suffix)
