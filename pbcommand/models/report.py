"""Common PacBio Report model


Author: Johann Miller and Michael Kocher
"""

from collections import defaultdict
import warnings
import abc
import logging
import json
import os
import re
from pprint import pformat

# make this optional. This is only for serialization
import numpy as np

log = logging.getLogger(__name__)

__all__ = ['PbReportError',
           'Attribute',
           'Report',
           'Plot',
           'PlotGroup',
           'Column',
           'Table']

import pbcommand

_HAS_NUMPY = False

try:
    import numpy as np
    _HAS_NUMPY = True
except ImportError:
    pass


def _get_decoder():
    """
    There's a bit of nonsense here to support the exiting pbreports python
    package.

    numpy is only used for Report that have Table columns that are numpy arrays.
    This really should have strictly defined in the original API to only support
    native python lists. Similarly with numpy scalars in Report Attributes.

    :return: None | numpy decoder
    """
    if _HAS_NUMPY:
        class NumpyJsonEncoder(json.JSONEncoder):

            def default(self, obj):
                if isinstance(obj, np.core.numerictypes.floating):
                    return float(obj)
                if isinstance(obj, np.core.numerictypes.integer):
                    return int(obj)
                if isinstance(obj, np.ndarray) and obj.ndim == 1:
                    return [float(x) for x in obj]
                # Let the base class default method raise the TypeError
                return json.JSONEncoder.default(self, obj)
        return NumpyJsonEncoder
    else:
        return None


def _to_json_with_decoder(d):
    decoder_or_none = _get_decoder()
    if decoder_or_none is None:
        return json.dumps(d, sort_keys=True, indent=4)
    else:
        return json.dumps(d, cls=decoder_or_none, sort_keys=True, indent=4)


class PbReportError(Exception):
    pass


class BaseReportElement(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, id_):
        if not isinstance(id_, basestring):
            raise PbReportError("Type error. id '{i}' cannot be {t}.".format(i=id_, t=type(id_)))

        if not re.match('^[a-z0-9_]+$', id_):
            msg = "id '{i}' for {x} must contain only alphanumeric or underscore characters".format(x=self.__class__.__name__, i=id_)
            log.error(msg)
            raise PbReportError(msg)

        self._id = id_
        self._ids = set([])

    def is_unique(self, id_):
        """
        Raise an error if a BaseReportElement with this id has already
        been added.
        :param id_: (int) id of child BaseReportElement
        """
        if id_ in self._ids:
            msg = "a plot with id '{i}' has already been added to {t}.".format(i=id_, t=str(type(self)))
            log.error(msg)
            raise PbReportError(msg)
        self._ids.add(id_)

    @property
    def id(self):
        return self._id

    @abc.abstractmethod
    def _get_attrs_simple(self):
        """
        Return a list of attributes names where each
        attribute returns a simple type like a string, int, or float.
        The 'id' attribute should NOT be included.
        Example [ 'title' ]
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _get_attrs_complex_list(self):
        """
        Return a list of attributes names where each
        attribute returns a list of BaseReportElement objects which
        implement to_dict()
        """
        raise NotImplementedError

    def to_dict(self, id_parts=None):
        """
        Return a dict-view of this object.
        Recursively descend in to collections of BaseReportElement instances,
        calling to_dict on each.

        Additionally, prepends the id with a '.'-delimited string of
        parent id's

        :param id_parts: (list of string)   Parent id's, as a function of depth within the object graph
        """
        if id_parts is None:
            # start the part list
            id_parts = [self.id]
        else:
            id_parts.append(self.id)

        d = {a: getattr(self, a) for a in self._get_attrs_simple()}

        # Versioning
        # import pbreports
        # version = pbreports.get_version()
        # changelist = pbreports.get_changelist()

        # d['_version'] = version
        # d['_changelist'] = changelist

        d['id'] = '.'.join([str(v) for v in id_parts])
        complex_attrs = self._get_attrs_complex_list()

        for ca in complex_attrs:
            d[ca] = []
            for i in getattr(self, ca):
                copy = []
                copy.extend(id_parts)
                d[ca].append(i.to_dict(copy))
                # yank the last id so it doesn't prepend the next item of same type.
                # slicing doesn't work on original list. need copy! bug 23799
                id_parts = copy[:-1]

            if len(id_parts) > 1:
                # yank the last id part, so it doesn't prepend the next
                # category of attributes
                id_parts = id_parts[:-1]
        return d


class Attribute(BaseReportElement):

    """
    An attribute always has an id and a value. A name is optional.
    """

    def __init__(self, id_, value, name=None):
        """
        :param id_: (str) Unique id for attribute (Not None, or Empty)
        :param value: (str, float) Numeric values should be float values. Formatting is performed durning the report rendering
        :param name: (str, None) optional display name. Can be changed in portal display rules
        """
        BaseReportElement.__init__(self, id_)
        if value is None:
            raise PbReportError("value cannot be None. {n} given.".format(n=value))
        self._value = value
        self._name = name

    @property
    def value(self):
        return self._value

    @property
    def name(self):
        return self._name

    def _get_attrs_simple(self):
        return ['value', 'name']

    def _get_attrs_complex_list(self):
        return []

    def __eq__(self, other):
        if isinstance(other, Attribute):
            if self.name == other.name and self.value == other.value and self.id == other.id:
                return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.id,
                  v=self.value,
                  n=self.name)
        return "<{k} id:{i} value:{v} name:{n} >".format(**_d)


class PlotGroup(BaseReportElement):

    """
    A plotGroup is a container of plots.
    """

    def __init__(self, id_, title=None, legend=None, thumbnail=None, plots=()):
        """
        :param id_: (str) id of plotgroup. Not None or Empty
        :param title: (str, None) Title of the plotGroup, displayed to user.
        :param legend: (str, None) Path to legend image, if applicable
        :param thumbnail: (str, None)Path to thumbnail image, if applicable
        :param plots: (list of Plot instances)
        """
        BaseReportElement.__init__(self, id_)
        self._title = title
        self._legend = legend
        self._thumbnail = thumbnail
        self._plots = []
        if plots:
            for plot in plots:
                self.add_plot(plot)

    @property
    def title(self):
        return self._title

    @property
    def legend(self):
        return self._legend

    @property
    def thumbnail(self):
        return self._thumbnail

    @property
    def plots(self):
        return self._plots

    @property
    def nplots(self):
        return len(self.plots)

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.id,
                  t=self.title,
                  n=self.nplots)
        return "<{k} id:{i} title:{t} nplots:{n} >".format(**_d)

    def _get_attrs_simple(self):
        return ['title', 'legend', 'thumbnail']

    def _get_attrs_complex_list(self):
        return ['plots']

    def add_plot(self, plot):
        """
        Add a plot to the plotGroup
        """
        if not isinstance(plot, Plot):
            raise TypeError("Unable to add plot. Got type {x} expect Plot".format(x=type(plot)))
        BaseReportElement.is_unique(self, plot.id)
        self._plots.append(plot)

    def to_dict(self, id_parts=None):
        return BaseReportElement.to_dict(self, id_parts=id_parts)


def _validate_not_abs_path(path):
    if os.path.isabs(path):
        raise ValueError("paths must be relative. Got {i}".format(i=path))


class Plot(BaseReportElement):

    """
    A plot contains a path to image file.
    """

    def __init__(self, id_, image, caption=None, thumbnail=None):
        """
        :param id_: (str, not None, or empty) Unique id for plot.
        :param image: (str) Required - not None - path to image
        :param caption: (str, None) Plot caption displayed to user under plot.
        :param thumbnail: (str, None) thumbnail path

        Paths must be given as relative
        """
        BaseReportElement.__init__(self, id_)

        if image is None:
            raise PbReportError('image cannot be None')
        _validate_not_abs_path(image)

        self._image = image
        self._caption = caption
        if thumbnail is not None:
            _validate_not_abs_path(thumbnail)

        self._thumbnail = thumbnail

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.id,
                  p=self.image)
        return "<{k} {i} {p} >".format(**_d)

    @property
    def image(self):
        return self._image

    @property
    def thumbnail(self):
        return self._thumbnail

    @property
    def caption(self):
        return self._caption

    def _get_attrs_simple(self):
        return ['image', 'caption']

    def _get_attrs_complex_list(self):
        return []


class Table(BaseReportElement):

    """
    A table consists of an id, title, and list of columns.
    """

    def __init__(self, id_, title=None, columns=()):
        """
        :param id_: (str), Unique id for table in report.
        :param title: (str, None)
        :param columns: (list of column instances)
        """
        BaseReportElement.__init__(self, id_)
        self._title = title
        self._columns = []
        if columns:
            for column in columns:
                self.add_column(column)

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.id,
                  t=self.title,
                  n=self.ncolumns)
        return "<{k} {i} title:{t} ncolumns:{n} >".format(**_d)

    def __str__(self):
        pad = 2
        max_values = max(len(column.values) for column in self.columns)
        # max length for each column value
        max_lengths = {}
        for c in self.columns:
            if c.values:
                n = max(max(len(str(v)) for v in c.values), len(c.header))
            else:
                n = len(c.header)
            max_lengths[c] = n

        header = "".join([c.header.ljust(max_lengths[c] + pad) for c in self.columns])

        outs = list()
        outs.append("")
        outs.append("Table id:{i}".format(i=self.id))
        outs.append("-" * len(header))
        outs.append(header)
        outs.append("-" * len(header))

        for i in xrange(max_values):
            out = []
            for column in self.columns:
                try:
                    l = max_lengths[column] + pad
                    out.append(str(column.values[i]).ljust(l))
                except IndexError as e:
                    log.warn(e)
                    out.append("No Value ")

            outs.append(" ".join(out))

        return "\n".join(outs)

    @property
    def id(self):
        return self._id

    @property
    def title(self):
        return self._title

    @property
    def ncolumns(self):
        return len(self.columns)

    @property
    def columns(self):
        return self._columns

    def _get_attrs_simple(self):
        return ['title']

    def _get_attrs_complex_list(self):
        return ['columns']

    def add_column(self, column):
        """
        Add a column to the table

        :param column: (Column instance)
        """
        if not isinstance(column, Column):
            raise TypeError("Got type {x}. Expected Column type.".format(x=type(column)))

        BaseReportElement.is_unique(self, column.id)
        self._columns.append(column)

    def append_data(self, column_index, item):
        """
        This should be deprecated in favor of `add_data_by_column_id`.

        Append datum to a column by column index

        :param column_index: (int) Index into internal column list
        :param item: (float, str) data item.
        """
        if column_index < len(self._columns):
            self._columns[column_index].values.append(item)
        else:
            raise IndexError("Unable to find index {i} in columns.".format(i=column_index))

    def add_data_by_column_id(self, column_id, value):
        """Add a value to column.

        :param column_id: (str) Column id
        :param value: (float, str, int)
        """
        if column_id in [c.id for c in self.columns]:
            # _columns should really be a dict
            # self._columns[column_id].values.append(value)
            for column in self.columns:
                if column_id == column.id:
                    column.values.append(value)
        else:
            raise KeyError("Unable to Column with id '{i}' to assign value {v}".format(i=column_id, v=value))

    @staticmethod
    def merge(tables):
        table_id = tables[0].id
        table_title = tables[0].title
        column_ids = sorted([col.id for col in tables[0].columns])

        col_collisions = {col_id: [] for col_id in column_ids}
        for table in tables:
            assert table.id == table_id
            assert table.title == table_title
            assert sorted([col.id for col in table.columns]) == column_ids
            for col in table.columns:
                col_collisions[col.id].append(col)
        columns = {}
        for col_id, cols in col_collisions.iteritems():
            assert len(cols) == len(tables)
            columns[col_id] = Column.merge(cols)
        # order by table[0]'s column order:
        columns = [columns[col.id] for col in tables[0].columns]
        return Table(table_id, table_title, columns=columns)


class Column(BaseReportElement):

    """
    A column consists of an id, header, and list of values.
    """

    def __init__(self, id_, header=None, values=()):
        """
        :param id_: (str)
        :param header: (str, None) Header of Column.
        """
        BaseReportElement.__init__(self, id_)
        self._id = id_
        self._header = header
        self._values = list(values)

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.id,
                  h=self.header,
                  n=self.nvalues)
        return "<{k} id:{i} header:{h} nvalues:{n} >".format(**_d)

    @property
    def id(self):
        return self._id

    @property
    def header(self):
        return self._header

    @property
    def nvalues(self):
        return len(self.values)

    @property
    def values(self):
        return self._values

    def _get_attrs_simple(self):
        return ['header', 'values']

    def _get_attrs_complex_list(self):
        return []

    @staticmethod
    def merge(columns):
        column_id = columns[0].id
        column_header = columns[0].header
        values = []
        for col in columns:
            assert col.id == column_id
            assert col.header == column_header
            values.extend(col.values)
        return Column(column_id, column_header, values=values)


class Report(BaseReportElement):

    """
    A report is a container for attributes, plotGroups, and tables.
    It can be serialized to json.
    """

    def __init__(self, id_, tables=(), attributes=(), plotgroups=(), dataset_uuids=()):
        """
        :param id_: (str) Should be a string that identifies the report, like 'adapter'.
        :param tables: (list of table instances)
        :param attributes: (list of attribute instances)
        :param plotgroups: (list of plot group instances)
        :param dataset_uuids: list[string] DataSet uuids of files used to generate the report
        """
        BaseReportElement.__init__(self, id_)
        self._attributes = []
        self._plotgroups = []
        self._tables = []
        if tables:
            for table in tables:
                self.add_table(table)
        if attributes:
            for attr in attributes:
                self.add_attribute(attr)
        if plotgroups:
            for plotgroup in plotgroups:
                self.add_plotgroup(plotgroup)

        # Datasets that
        self._dataset_uuids = dataset_uuids

    def add_attribute(self, attribute):
        """Add an attribute to the report
        :param attribute: (Attribute instance)
        """
        if not isinstance(attribute, Attribute):
            TypeError("Got type {x}. Expected Attribute type.".format(x=type(attribute)))

        BaseReportElement.is_unique(self, attribute.id)
        self._attributes.append(attribute)

    def add_plotgroup(self, plotgroup):
        """
        Add a plotgroup to the report
        """
        if not isinstance(plotgroup, PlotGroup):
            TypeError("Got type {x}. Expected Attribute type.".format(x=type(plotgroup)))

        BaseReportElement.is_unique(self, plotgroup.id)
        self._plotgroups.append(plotgroup)

    def add_table(self, table):
        """
        Add a table to the report
        """
        BaseReportElement.is_unique(self, table.id)
        self._tables.append(table)

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.id,
                  a=len(self.attributes),
                  p=len(self.plotGroups),
                  t=len(self.tables))
        return "<{k} id:{i} nattributes:{a} nplot_groups:{p} ntables:{t} >".format(**_d)

    @property
    def attributes(self):
        return self._attributes

    @property
    def plotGroups(self):
        return self._plotgroups

    @property
    def tables(self):
        return self._tables

    def _get_attrs_simple(self):
        return []

    def _get_attrs_complex_list(self):
        return ['attributes', 'plotGroups', 'tables']

    def get_attribute_by_id(self, id_):
        """Get an attribute by id. The id should NOT contain the root report id

        :returns: (None, Attribute)

        Example:
        report.get_attribute_by_id('nmovies')

        *NOT*
        report.get_attribute_by_id('overview.nmovies')
        """
        for attr in self.attributes:
            if attr.id == id_:
                return attr

        return None

    def to_dict(self, id_parts=None):
        version = pbcommand.get_version()

        d = BaseReportElement.to_dict(self, id_parts=id_parts)
        d['_version'] = version
        d['_changelist'] = "UNKNOWN"
        d['dataset_uuids'] = list(set(self._dataset_uuids))
        return d

    def to_json(self):
        """Return a json string of the report"""
        try:
            s = _to_json_with_decoder(self.to_dict())
        except TypeError as e:
            msg = "Unable to serialize report due to {e} \n".format(e=e)
            log.error(msg)
            log.error("Object: " + pformat(self.to_dict()))
            raise

        return s

    def write_json(self, file_name):
        """
        Serialized the report to a json file.

        :param file_name: (str) Path to write output json file to.
        """
        with open(file_name, 'w') as f:
            f.write(self.to_json())
        log.info("Wrote report {r}".format(r=file_name))

    @staticmethod
    def from_simple_dict(report_id, raw_d, namespace):
        """
        Generate a Report with populated attributes, starting from a flat
        dictionary (without namespace).
        """
        attributes = []
        for k, v in raw_d.items():
            ns = "_".join([namespace, k.lower()])
            # These can't be none for some reason
            if v is not None:
                a = Attribute(ns, v, name=k)
                attributes.append(a)
            else:
                warnings.warn("skipping null entry {k}->{v}".format(k=k, v=v))
        return Report(report_id, attributes=attributes)

    @staticmethod
    def merge(reports):
        report_id = reports[0].id
        def _merge_attributes_d(attributes_list):
            attrs = defaultdict(lambda : [])
            for ax in attributes_list:
                for a in ax:
                    attrs[a.id].append(a.value)
            return attrs
        def _attributes_to_table(attributes_list, table_id, title):
            attrs = _merge_attributes_d(attributes_list)
            columns = [ Column(k.lower(), header=k, values=values)
                        for k, values in attrs.iteritems() ]
            table = Table(table_id, title=title, columns=columns)
            return table
        def _sum_attributes(attributes_list):
            d = _merge_attributes_d(attributes_list)
            return [ Attribute(k, sum(values), name=k)
                     for k, values in d.iteritems() ]
        def _merge_tables(tables):
            """Pass through singletons, Table.merge dupes"""
            id_collisions = defaultdict(list)
            merged = []
            for tab in tables:
                id_collisions[tab.id].append(tab)
            for tabs in id_collisions.values():
                if len(tabs) == 1:
                    merged.append(tabs[0])
                else:
                    merged.append(Table.merge(tabs))
            return merged
        attr_list = []
        table_list = []
        for report in reports:
            assert report.id == report_id
            attr_list.append(report.attributes)
            table_list.extend(report.tables)
        table = _attributes_to_table(attr_list, 'chunk_metrics',
                                     "Chunk Metrics")
        tables = _merge_tables(table_list)
        tables.append(table)
        merged_attributes = _sum_attributes(attr_list)
        return Report(report_id, attributes=merged_attributes, tables=tables)
