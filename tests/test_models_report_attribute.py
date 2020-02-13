import logging

import pytest

from pbcommand.models.report import Attribute, PbReportError

log = logging.getLogger(__name__)


class TestAttribute:

    def test_attribute_null_id(self):
        """Can't create an attribute without an id."""
        def _test():
            a = Attribute(None, 1)

        with pytest.raises(PbReportError):
            _test()

    def test_attribute_int_id(self):
        """Test exception of handling Attribute with int ids"""
        def _test():
            a = Attribute(1, 12345)

        with pytest.raises(PbReportError):
            _test()

    def test_to_dict(self):
        """
        Test attribute to_dict function
        """
        a = Attribute('bob', 123, "Bob is the name")
        d = a.to_dict()
        assert 'bob' == d['id']
        assert 123 == d['value']
        assert 'Bob is the name' == d['name']

    def test_eq(self):
        a = Attribute('a', 1234, "My Attribute")
        b = Attribute('b', 1234, "My B Attribute")
        c = Attribute('a', 1234, "My Attribute")
        assert a == c
        assert a != b
        assert b != c

    def test_repr(self):
        a = Attribute('a', 1234, "My Attribute")
        log.info(repr(a))
        assert repr(a) is not None
