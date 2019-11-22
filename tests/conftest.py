import pytest


def pytest_runtest_setup(item):
    for mark in item.iter_markers():
        if mark.name == 'pbcore':
            try:
                import pbcore
            except ImportError:
                pytest.skip("'pbcore' not installed")
        elif mark.name == 'pbtestdata':
            try:
                import pbtestdata
            except ImportError:
                pytest.skip("'pbtestdata' not installed")
        else:
            raise LookupError("Unknown pytest mark: '{}'".format(mark.name))
