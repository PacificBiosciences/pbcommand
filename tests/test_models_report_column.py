import logging
import unittest

from pbcommand.models.report import Column

log = logging.getLogger(__name__)


class TestColumn(unittest.TestCase):

    def test_column(self):
        """Test: Can't create a Column without an id."""
        def none_col():
            c = Column(None)

        self.assertRaises(none_col)

    def test_repr(self):
        c = Column('my_column', header="My Column", values=list(xrange(5)))
        self.assertIsNotNone(repr(c))

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
#            self.assertEqual( 'foo.id', pg.plots[0].id )
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
#            self.assertEquals( 123, d['id'] )
#            self.assertEquals( 'foo title', d['title'] )
#            self.assertEquals( 'foo legend', d['legend'] )
#            self.assertEquals( 'foo thumbnail', d['thumbnail'] )
#            self.assertEquals( 1, len(d['plots']) )
#        except:
#            log.error(traceback.format_exc())
#            raise
#
#
#
