import logging
import unittest

from pbcommand.models.report import Table, Column, PbReportError

log = logging.getLogger(__name__)


class TestEmptyTable(unittest.TestCase):

    """Basic Smoke tests"""

    def setUp(self):
        self.columns = [Column('one', header="One"),
                        Column('two', header="Two"),
                        Column('three', header="Three")]

        self.table = Table('my_table', columns=self.columns)

    def test_str(self):
        """Smoke test for conversion to str"""
        log.info(str(self.table))
        self.assertIsNotNone(str(self.table))

    def test_columns(self):
        """Test Columns"""
        self.assertEqual(len(self.table.columns), 3)

    def test_column_values(self):
        """Basic check for column values"""
        for column in self.table.columns:
            self.assertEqual(len(column.values), 0)

    def test_to_dict(self):
        """Conversion to dictionary"""
        self.assertTrue(isinstance(self.table.to_dict(), dict))
        log.info(self.table.to_dict())


class TestBasicTable(unittest.TestCase):

    """Basic Smoke tests"""

    def setUp(self):
        self.columns = [Column('one', header="One"),
                        Column('two', header="Two"),
                        Column('three', header="Three")]
        self.table = Table('my_table_with_values', columns=self.columns)
        datum = {'one': list(xrange(3)), 'two': list('abc'),
                 'three': 'file1 file2 file3'.split()}
        for k, values in datum.iteritems():
            for value in values:
                self.table.add_data_by_column_id(k, value)

    def test_str(self):
        """Smoke test for conversion to str"""
        log.info(str(self.table))
        self.assertIsNotNone(str(self.table))

    def test_columns(self):
        """Test Columns"""
        self.assertEqual(len(self.table.columns), 3)

    def test_column_values(self):
        """Basic check for column values"""
        for column in self.table.columns:
            self.assertEqual(len(column.values), 3)

    def test_to_dict(self):
        """Conversion to dictionary"""
        self.assertTrue(isinstance(self.table.to_dict(), dict))
        log.info(self.table.to_dict())


class TestTable(unittest.TestCase):

    def test_table(self):
        """Can't create an Table without an id."""
        def none_table():
                t = Table(None)
        self.assertRaises(none_table)

    def test_add_column(self):
        """Cannot add column with duplicate id."""
        cs = [Column('1'), Column('2')]
        t = Table('foo', columns=cs)

        def add_dupe():
            t.add_column(Column('2'))

        self.assertSequenceEqual(cs, t.columns)

        self.assertRaises(PbReportError, add_dupe)

    def test_append_data(self):
        """Append data to columns by index."""

        cs = [Column('1'), Column('2')]
        t = Table('foo', columns=cs)

        t.append_data(0, 'whatev')
        t.append_data(0, 'huh')
        t.append_data(1, 'ernie')
        t.append_data(1, 'bert')

        self.assertSequenceEqual(['whatev', 'huh'], t.columns[0].values)
        self.assertSequenceEqual(['ernie', 'bert'], t.columns[1].values)

    def test_add_data_by_column_id(self):
        """Added data values by column identifier."""

        columns = [Column('one'), Column('two')]
        table = Table('mytable', columns=columns)

        datum = {'one': 12.0, 'two': 1234.0}

        for k, v in datum.iteritems():
            table.add_data_by_column_id(k, v)

        self.assertTrue(12.0 in table.columns[0].values)
        self.assertTrue(1234.0 in table.columns[1].values)
