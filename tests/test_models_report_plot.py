import logging
from pprint import pformat

import pytest

from pbcommand.models.report import Plot, PbReportError, PlotlyPlot

log = logging.getLogger(__name__)


class TestPlot:

    def test_plot_null_id(self):
        """Can't create an plot without an id."""
        with pytest.raises(PbReportError):
            p = Plot(None, 'foo')

    def test_plot_null_image(self):
        """Can't create an plot without an image."""
        def _test():
            p = Plot('123', None)
        with pytest.raises(PbReportError):
            _test()

    def test_to_dict(self):
        """Test plot to dictionary method"""
        a = Plot('123', 'foo', caption='foo is the caption')
        d = a.to_dict()
        assert '123' == d['id']
        assert 'foo' == d['image']
        assert 'foo is the caption' == d['caption']
        assert Plot.PLOT_TYPE == d['plotType']
        log.info(pformat(d, indent=4))
        log.info(repr(a))
        assert repr(a) is not None

    def test_init_with_thumbnail(self):
        """Initial with thumbnail"""
        image = "my_image.png"
        thumbnail = "my_image_thumb.png"
        p = Plot('plot_1', image, thumbnail=thumbnail, caption="Awesome image")

        assert p.thumbnail == thumbnail
        log.info(pformat(p.to_dict()))
        assert isinstance(p.to_dict(), dict)

    def test_plotly_plot(self):
        a = PlotlyPlot('123', 'foo', caption='foo is the caption',
                       plotly_version="1.2.3")
        d = a.to_dict()
        assert '123' == d['id']
        assert 'foo' == d['image']
        assert 'foo is the caption' == d['caption']
        assert d['plotType'] == PlotlyPlot.PLOT_TYPE
        assert d['plotlyVersion'] == "1.2.3"
