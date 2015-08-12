import unittest
from pbcommand.utils import Singleton


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
