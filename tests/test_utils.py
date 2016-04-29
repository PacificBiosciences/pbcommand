import functools
import tempfile
import unittest
import argparse
import logging

from pbcommand.utils import (Singleton, compose, get_parsed_args_log_level,
    get_dataset_metadata)


class TestSingleton(unittest.TestCase):

    def test_basic(self):
        class Lithium(object):
            __metaclass__ = Singleton

            def __init__(self):
                self.name = 'Lithium'
                self.number = 3

        a = Lithium()
        b = Lithium()
        self.assertEqual(id(a), id(b))


class TestCompose(unittest.TestCase):
    def test_simple(self):
        f = lambda x: x * 2
        g = lambda y: y + 2

        h = compose(f, g)
        value = h(7)
        self.assertEquals(value, 18)

    def test_no_args_list(self):

        def _f():
            return compose()

        self.assertRaises(ValueError, _f)

    def test_empty_list(self):
        def _f():
            return compose([])

        self.assertRaises(TypeError, _f)

    def test_partial(self):

        def add(a, b):
            return a + b

        add_five = functools.partial(add, 5)
        add_two = functools.partial(add, 2)

        f = compose(add_five, add_two)
        value = f(5)
        self.assertEquals(value, 12)


class TestLogging(unittest.TestCase):

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
        self.assertEqual(l, logging.INFO)
        p = _get_argparser().parse_args(["--quiet"])
        l = get_parsed_args_log_level(p)
        self.assertEqual(l, logging.ERROR)
        p = _get_argparser().parse_args(["--debug"])
        l = get_parsed_args_log_level(p)
        self.assertEqual(l, logging.DEBUG)
        p = _get_argparser("ERROR").parse_args(["--verbose"])
        l = get_parsed_args_log_level(p)
        self.assertEqual(l, logging.INFO)
        p = _get_argparser("DEBUG").parse_args(["--log-level=WARNING"])
        l = get_parsed_args_log_level(p)
        self.assertEqual(l, logging.WARNING)
        p = _get_argparser("NOTSET").parse_args([])
        l = get_parsed_args_log_level(p)
        self.assertEqual(l, logging.NOTSET)
        p = _get_argparser(logging.NOTSET).parse_args([])
        l = get_parsed_args_log_level(p)
        self.assertEqual(l, logging.NOTSET)


class TestUtils(unittest.TestCase):

    def test_get_dataset_metadata(self):
        try:
            import pbcore.io
            import pbcore.data
        except ImportError:
            raise unittest.SkipTest("pbcore not available, skipping")
        else:
            ds = pbcore.io.SubreadSet(pbcore.data.getUnalignedBam())
            ds_file = tempfile.NamedTemporaryFile(suffix=".subreadset.xml").name
            ds.write(ds_file)
            md = get_dataset_metadata(ds_file)
            self.assertEqual(md.metatype, "PacBio.DataSet.SubreadSet")
            self.assertEqual(md.uuid, ds.uuid)
