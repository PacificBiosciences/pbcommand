
import unittest
import logging

from pbcommand.models.report import Attribute, PbReportError

log = logging.getLogger(__name__)

class TestAttribute(unittest.TestCase):

    def test_attribute_null_id(self):
        """Can't create an attribute without an id."""
        def _test():
            a = Attribute(None, 1)

        self.assertRaises(PbReportError, _test)

    def test_attribute_null_value(self):
        """Can't create an attribute without a value."""
        def _test():
            a = Attribute('bob', None)

        self.assertRaises(PbReportError, _test)

    def test_attribute_int_id(self):
        """Test exception of handling Attribute with int ids"""
        def _test():
            a = Attribute(1, 12345)

        self.assertRaises(PbReportError, _test)

    def test_to_dict(self):
        """
        Test attribute to_dict function
        """
        a = Attribute('bob', 123, "Bob is the name")
        d = a.to_dict()
        self.assertEquals('bob', d['id'])
        self.assertEquals(123, d['value'])
        self.assertEquals('Bob is the name', d['name'])

    def test_eq(self):
        a = Attribute('a', 1234, "My Attribute")
        b = Attribute('b', 1234, "My B Attribute")
        c = Attribute('a', 1234, "My Attribute")
        self.assertTrue(a == c)
        self.assertTrue(a != b)
        self.assertTrue(b != c)

    def test_repr(self):
        a = Attribute('a', 1234, "My Attribute")
        log.info(repr(a))
        self.assertIsNotNone(repr(a))
