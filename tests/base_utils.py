
import os

from pbcommand.testkit.base_utils import *

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data'))


def get_data_file(path):
    return os.path.join(DATA_DIR, path)


def get_data_file_from_subdir(subdir, path):
    return os.path.join(DATA_DIR, subdir, path)
