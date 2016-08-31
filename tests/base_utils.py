
import os

from pbcommand.testkit.base_utils import *

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data'))

DATA_DIR_TC = os.path.join(DATA_DIR, 'tool-contracts')
DATA_DIR_RTC = os.path.join(DATA_DIR, 'resolved-tool-contracts')
DATA_DIR_PRESETS = os.path.join(DATA_DIR, "pipeline-presets")
DATA_DIR_DSVIEW = os.path.join(DATA_DIR, "pipeline-datastore-view-rules")
DATA_DIR_REPORT_SPECS = os.path.join(DATA_DIR, "report-specs")


def get_data_file(path):
    return os.path.join(DATA_DIR, path)


def get_data_file_from_subdir(subdir, path):
    return os.path.join(DATA_DIR, subdir, path)


def get_tool_contract(name):
    return os.path.join(DATA_DIR_TC, name)


def get_resolved_tool_contract(name):
    return os.path.join(DATA_DIR_RTC, name)
