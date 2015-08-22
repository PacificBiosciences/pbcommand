import functools
import unittest
from pbcommand.utils import Singleton, compose


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