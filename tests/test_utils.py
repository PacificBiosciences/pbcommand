import argparse
import functools
import logging
import tempfile

import pytest

from pbcommand.utils import (Singleton, compose, get_parsed_args_log_level,
                             get_dataset_metadata)


class TestSingleton:

    def test_basic(self):
        class Lithium(metaclass=Singleton):
            def __init__(self):
                self.name = 'Lithium'
                self.number = 3

        a = Lithium()
        b = Lithium()
        assert id(a) == id(b)


class TestCompose:
    def test_simple(self):
        def f(x): return x * 2
        def g(y): return y + 2

        h = compose(f, g)
        value = h(7)
        assert value == 18

    def test_no_args_list(self):

        def _f():
            return compose()

        with pytest.raises(ValueError):
            _f()

    def test_empty_list(self):
        def _f():
            return compose([])

        with pytest.raises(TypeError):
            _f()

    def test_partial(self):

        def add(a, b):
            return a + b

        add_five = functools.partial(add, 5)
        add_two = functools.partial(add, 2)

        f = compose(add_five, add_two)
        value = f(5)
        assert value == 12


class TestLogging:

    def test_get_parsed_args_log_level(self):
        # XXX more of an integration test, sorry - we need to ensure that
        # these functions work in combination with get_parsed_args_log_level
        from pbcommand.common_options import (
            add_log_debug_option, add_log_quiet_option, add_log_verbose_option,
            add_log_level_option)

        def _get_argparser(level="INFO"):
            p = argparse.ArgumentParser()
            p.add_argument("--version", action="store_true")
            add_log_level_option(add_log_debug_option(add_log_quiet_option(
                add_log_verbose_option(p))), default_level=level)
            return p
        p = _get_argparser().parse_args([])
        l = get_parsed_args_log_level(p)
        assert l == logging.INFO
        p = _get_argparser().parse_args(["--quiet"])
        l = get_parsed_args_log_level(p)
        assert l == logging.ERROR
        p = _get_argparser().parse_args(["--debug"])
        l = get_parsed_args_log_level(p)
        assert l == logging.DEBUG
        p = _get_argparser("ERROR").parse_args(["--verbose"])
        l = get_parsed_args_log_level(p)
        assert l == logging.INFO
        p = _get_argparser("DEBUG").parse_args(["--log-level=WARNING"])
        l = get_parsed_args_log_level(p)
        assert l == logging.WARNING
        p = _get_argparser("NOTSET").parse_args([])
        l = get_parsed_args_log_level(p)
        assert l == logging.NOTSET
        p = _get_argparser(logging.NOTSET).parse_args([])
        l = get_parsed_args_log_level(p)
        assert l == logging.NOTSET


class TestUtils:

    @pytest.mark.pbcore
    @pytest.mark.pbtestdata
    def test_get_dataset_metadata(self):
        import pbtestdata
        md = get_dataset_metadata(pbtestdata.get_file("subreads-xml"))
        assert md.metatype == "PacBio.DataSet.SubreadSet"
        assert md.name == "subreads-xml"

        from pbcore.io import SubreadSet
        ds = SubreadSet(pbtestdata.get_file("subreads-xml"))
        assert md.uuid == ds.uuid

        with pytest.raises(Exception) as e:
            get_dataset_metadata(None)
