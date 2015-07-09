""" Commandline Parser for Tools. Supports Tool Contracts

# Author: Michael Kocher
"""
import logging
import argparse

log = logging.getLogger(__name__)

__version__ = "0.1.0"


def get_default_argparser(version, description):
    """
    Everyone MUST use this to create an instance on a argparser python parser.

    :param version:
    :param description:
    :return:
    :rtype: ArgumentParser
    """
    p = argparse.ArgumentParser(version=version,
                                description=description,
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    return p


