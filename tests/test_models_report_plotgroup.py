import logging
import unittest
from pprint import pformat

from pbcommand.models.report import PlotGroup, Plot, PbReportError

log = logging.getLogger(__name__)


class TestPlotGroup(unittest.TestCase):

    def test_init(self):
        """Test constructor with kwargs"""
        plot = Plot('a_plot', 'path/to/image.png', caption="My Image")
        p = PlotGroup('my_pg', plots=[plot])
        self.assertIsNotNone(p)

    def test_plotgroup_null_id(self):
        """Can't create an plotGroup without an id."""
        def _test():
            p = PlotGroup(None)

        self.assertRaises(PbReportError, _test)

    def test_plotgroup_add_duplicate_plot(self):
        """Can't add plots with duplicate ids."""
        def _test():
            pg = PlotGroup('foo')
            pg.add_plot(Plot('id', 'i1'))
            pg.add_plot(Plot('id', 'i2'))

        self.assertRaises(PbReportError, _test)

    def test_to_dict(self):
        """Test plotGroup to_dict function."""
        a = PlotGroup('123', title='foo title', legend='foo legend',
                      thumbnail='foo thumbnail')
        a.add_plot(Plot('id', 'i1', caption='a caption'))

        d = a.to_dict()
        log.debug(pformat(d))

        self.assertEqual('123', d['id'])
        self.assertEqual('foo title', d['title'])
        self.assertEqual('foo legend', d['legend'])
        self.assertEqual('foo thumbnail', d['thumbnail'])
        self.assertEqual(1, len(d['plots']))
        log.info(a)
        self.assertIsNotNone(repr(a))

    def test_adding_incorrect_type(self):
        """Validate type when adding Plots."""
        def _test():
            plots = ['Not a plot instance', 'Another bad plot.']
            p = PlotGroup('my_plotgroup', plots=plots)

        self.assertRaises(TypeError, _test)
