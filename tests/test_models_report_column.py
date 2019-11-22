import logging

import pytest

from pbcommand.models.report import Column

log = logging.getLogger(__name__)


class TestColumn:

    def test_column(self):
        """Test: Can't create a Column without an id."""
        def none_col():
            c = Column(None)

        with pytest.raises(Exception):
            none_col()

    def test_repr(self):
        c = Column('my_column', header="My Column", values=list(range(5)))
        assert repr(c) is not None

#    def test_plotgroup_add_duplicate_plot(self):
#        '''
#        Test: Can't add plots with duplicate ids
#        '''
#        try:
#            log.info( TestPlotGroup.test_plotgroup_add_duplicate_plot.__doc__ )
#            pg = PlotGroup('foo')
#            pg.add_plot(Plot('id', 'i1'))
#
#            try:
#                pg.add_plot( Plot('id', 'i2') )
#                self.fail( 'Cannot add plot with same id' )
#            except PbReportError:
#                pass
#        except:
#            log.error(traceback.format_exc())
#            raise
#
#
#
#    def test_plotgroup_id_prepend(self):
#        '''
#        Test: PlotGroup id gets prepended to plot.id when plot is added
#        '''
#        try:
#            log.info( TestPlotGroup.test_plotgroup_id_prepend.__doc__ )
#            pg = PlotGroup('foo')
#            pg.add_plot( Plot('id', 'i1') )
#            assert 'foo.id' == pg.plots[0].id
#        except:
#            log.error(traceback.format_exc())
#            raise
#
#
#    def test_to_dict(self):
#        '''
#        Test plotGroup to_dict function
#        '''
#        try:
#            log.info( TestPlotGroup.test_to_dict.__doc__ )
#            a = PlotGroup(123, 'foo title', 'foo legend', 'foo thumbnail' )
#            a.add_plot( Plot('id', 'i1') )
#
#            d = a.to_dict()
#            assert 123 == d['id']
#            assert 'foo title' == d['title']
#            assert 'foo legend' == d['legend']
#            assert 'foo thumbnail' == d['thumbnail']
#            assert 1 == len(d['plots'])
#        except:
#            log.error(traceback.format_exc())
#            raise
#
#
#
