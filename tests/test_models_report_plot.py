import logging
import unittest
from pprint import pformat

from pbcommand.models.report import Plot, PbReportError

log = logging.getLogger(__name__)


class TestPlot(unittest.TestCase):

    def test_plot_null_id(self):
        """Can't create an plot without an id."""
        with self.assertRaises(PbReportError):
            p = Plot(None, 'foo')

    def test_plot_null_image(self):
        """Can't create an plot without an image."""
        def _test():
            p = Plot('123', None)
        self.assertRaises(PbReportError, _test)

    def test_to_dict(self):
        """Test plot to dictionary method"""
        a = Plot('123', 'foo', caption='foo is the caption')
        d = a.to_dict()
        self.assertEquals('123', d['id'])
        self.assertEquals('foo', d['image'])
        self.assertEquals('foo is the caption', d['caption'])
        log.info(pformat(d, indent=4))
        log.info(repr(a))
        self.assertIsNotNone(repr(a))

    def test_init_with_thumbnail(self):
        """Initial with thumbnail"""
        image = "my_image.png"
        thumbnail = "my_image_thumb.png"
        p = Plot('plot_1', image, thumbnail=thumbnail, caption="Awesome image")

        self.assertEqual(p.thumbnail, thumbnail)
        log.info(pformat(p.to_dict()))
        self.assertTrue(isinstance(p.to_dict(), dict))

