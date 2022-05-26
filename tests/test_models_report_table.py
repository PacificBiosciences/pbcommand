import logging
import tempfile

import pytest

from pbcommand.models.report import Table, Column, PbReportError

log = logging.getLogger(__name__)


class TestEmptyTable:

    """Basic Smoke tests"""

    def setup_method(self, method):
        self.columns = [Column('one', header="One"),
                        Column('two', header="Two"),
                        Column('three', header="Three")]

        self.table = Table('my_table', columns=self.columns)

    def test_str(self):
        """Smoke test for conversion to str"""
        log.info(str(self.table))
        assert str(self.table) is not None

    def test_columns(self):
        """Test Columns"""
        assert len(self.table.columns) == 3

    def test_column_values(self):
        """Basic check for column values"""
        for column in self.table.columns:
            assert len(column.values) == 0

    def test_to_dict(self):
        """Conversion to dictionary"""
        assert isinstance(self.table.to_dict(), dict)
        log.info(self.table.to_dict())


class TestBasicTable:

    """Basic Smoke tests"""

    def setup_method(self, method):
        self.columns = [Column('one', header="One"),
                        Column('two', header="Two"),
                        Column('three', header="Three"),
                        Column('four', header="Four")]
        self.table = Table('my_table_with_values', columns=self.columns)
        datum = [
            ('one', list(range(3))),
            ('two', list('abc')),
            ('three', 'file1 file2 file3'.split()),
            ('four', [3.14159, 2.72, 9.87654321])
        ]
        for k, values in datum:
            for value in values:
                self.table.add_data_by_column_id(k, value)

    def test_str(self):
        """Smoke test for conversion to str"""
        log.info(str(self.table))
        assert str(self.table) is not None

    def test_columns(self):
        """Test Columns"""
        assert len(self.table.columns) == 4

    def test_column_values(self):
        """Basic check for column values"""
        for column in self.table.columns:
            assert len(column.values) == 3

    def test_to_dict(self):
        """Conversion to dictionary"""
        assert isinstance(self.table.to_dict(), dict)
        log.info(self.table.to_dict())

    def test_to_csv(self):
        f = tempfile.NamedTemporaryFile(suffix=".csv").name
        self.table.to_csv(f)
        with open(f) as csv_out:
            assert csv_out.read() == "One,Two,Three,Four\n0,a,file1,3.14159\n1,b,file2,2.72\n2,c,file3,9.87654321\n"
        self.table.to_csv(f, float_format="%.4f")
        with open(f) as csv_out:
            assert csv_out.read() == "One,Two,Three,Four\n0,a,file1,3.1416\n1,b,file2,2.7200\n2,c,file3,9.8765\n"
        self.table.to_csv(f, float_format="{:.2f}")
        with open(f) as csv_out:
            assert csv_out.read() == "One,Two,Three,Four\n0,a,file1,3.14\n1,b,file2,2.72\n2,c,file3,9.88\n"


class TestTable:

    def test_table(self):
        """Can't create an Table without an id."""
        def none_table():
            t = Table(None)
        with pytest.raises(Exception):
            none_table()

    def test_add_column(self):
        """Cannot add column with duplicate id."""
        cs = [Column('1'), Column('2')]
        t = Table('foo', columns=cs)

        def add_dupe():
            t.add_column(Column('2'))

        assert cs == t.columns

        with pytest.raises(PbReportError):
            add_dupe()

    def test_append_data(self):
        """Append data to columns by index."""

        cs = [Column('1'), Column('2')]
        t = Table('foo', columns=cs)

        t.append_data(0, 'whatev')
        t.append_data(0, 'huh')
        t.append_data(1, 'ernie')
        t.append_data(1, 'bert')

        assert ['whatev', 'huh'] == t.columns[0].values
        assert ['ernie', 'bert'] == t.columns[1].values

    def test_add_data_by_column_id(self):
        """Added data values by column identifier."""

        columns = [Column('one'), Column('two')]
        table = Table('mytable', columns=columns)

        datum = {'one': 12.0, 'two': 1234.0}

        for k, v in datum.items():
            table.add_data_by_column_id(k, v)

        assert 12.0 in table.columns[0].values
        assert 1234.0 in table.columns[1].values
