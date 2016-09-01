"""Common PacBio Report model


Author: Johann Miller and Michael Kocher
"""

from collections import defaultdict, OrderedDict
import warnings
import abc
import logging
import json
import os
import re
import uuid as U  # to allow use of uuid as local var
from pprint import pformat
import datetime

import pbcommand


log = logging.getLogger(__name__)

__all__ = ['PbReportError',
           'Attribute',
           'Report',
           'Plot',
           'PlotGroup',
           'Column',
           'Table']

# If/when the Report datamodel change, this needs to be changed using
# the semver model
PB_REPORT_SCHEMA_VERSION = "1.0.0"

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
        return json.dumps(d, sort_keys=True, indent=4, separators=(',', ': '))
    else:
        return json.dumps(d, cls=decoder_or_none, sort_keys=True, indent=4, separators=(',', ': '))


class PbReportError(Exception):
    pass


class BaseReportElement(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, id_):
        if not isinstance(id_, basestring):
            raise PbReportError(
                "Type error. id '{i}' cannot be {t}.".format(i=id_, t=type(id_)))

        if not re.match('^[a-z0-9_]+$', id_):
            msg = "id '{i}' for {x} must contain only lower-case alphanumeric or underscore characters".format(
                x=self.__class__.__name__, i=id_)
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
            msg = "a plot with id '{i}' has already been added to {t}.".format(
                i=id_, t=str(type(self)))
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
            raise PbReportError(
                "value cannot be None. {n} given.".format(n=value))
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

    def get_plot_by_id(self, id_):

        for plot in self.plots:
            if plot.id == id_:
                return plot

        return None

    def add_plot(self, plot):
        """
        Add a plot to the plotGroup
        """
        if not isinstance(plot, Plot):
            raise TypeError(
                "Unable to add plot. Got type {x} expect Plot".format(x=type(plot)))
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

    def __init__(self, id_, image, caption=None, thumbnail=None, title=None):
        """
        :param id_: (str, not None, or empty) Unique id for plot.
        :param image: (str) Required - not None - path to image
        :param caption: (str, None) Plot caption displayed to user under plot.
        :param thumbnail: (str, None) thumbnail path
        :param title: str Display Name of the Plot

        Paths must be given as relative
        """
        BaseReportElement.__init__(self, id_)

        if image is None:
            raise PbReportError('image cannot be None')
        _validate_not_abs_path(image)

        self._image = image
        self._caption = caption
        self.title = title
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
        return ['image', 'caption', 'title']

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
        headers = []
        for c in self.columns:
            this_header = ""
            if c.header is not None:
                this_header = c.header
            if c.values:
                n = max(max(len(str(v)) for v in c.values), len(this_header))
            else:
                n=len(this_header)
            max_lengths[c] = n
            headers.append(this_header)

        header="".join([h.ljust(max_lengths[c] + pad)
                          for h in headers])

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

    def get_column_by_id(self, id_):

        for col in self.columns:
            if col.id == id_:
                return col

        return None

    def add_column(self, column):
        """
        Add a column to the table

        :param column: (Column instance)
        """
        if not isinstance(column, Column):
            raise TypeError(
                "Got type {x}. Expected Column type.".format(x=type(column)))

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
            raise IndexError(
                "Unable to find index {i} in columns.".format(i=column_index))

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
            raise KeyError("Unable to Column with id '{i}' to assign value {v}".format(
                i=column_id, v=value))

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

    def __init__(self, id_, title=None, tables=(), attributes=(), plotgroups=(), dataset_uuids=(), uuid=None):
        """
        :param id_: (str) Should be a string that identifies the report, like 'adapter'.
        :param title: Display name of report Defaults to the Report+id if None (added in 0.3.9)
        :param tables: (list of table instances)
        :param attributes: (list of attribute instances)
        :param plotgroups: (list of plot group instances)
        :param dataset_uuids: list[string] DataSet uuids of files used to generate the report
        :param uuid: the unique identifier for the Report
        """
        BaseReportElement.__init__(self, id_)
        self._attributes = []
        self._plotgroups = []
        self._tables = []
        self.title = "Report {i}".format(i=self.id) if title is None else title
        # FIXME(mkocher)(2016-3-30) Add validation to make sure it's a well formed value
        # this needs to be required
        self.uuid = uuid if uuid is not None else str(U.uuid4())

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

    @property
    def dataset_uuids(self):
        return self._dataset_uuids

    def add_attribute(self, attribute):
        """Add an attribute to the report
        :param attribute: (Attribute instance)
        """
        if not isinstance(attribute, Attribute):
            TypeError("Got type {x}. Expected Attribute type.".format(
                x=type(attribute)))

        BaseReportElement.is_unique(self, attribute.id)
        self._attributes.append(attribute)

    def add_plotgroup(self, plotgroup):
        """
        Add a plotgroup to the report
        """
        if not isinstance(plotgroup, PlotGroup):
            TypeError("Got type {x}. Expected Attribute type.".format(
                x=type(plotgroup)))

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
                  n=self.title,
                  a=len(self.attributes),
                  p=len(self.plotGroups),
                  t=len(self.tables), u=self.uuid)
        return "<{k} id:{i} title:{n} uuid:{u} nattributes:{a} nplot_groups:{p} ntables:{t} >".format(**_d)

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

    def get_table_by_id(self, id_):

        for table in self.tables:
            if table.id == id_:
                return table

        return None

    def get_plotgroup_by_id(self, id_):

        for pg in self.plotGroups:
            if pg.id == id_:
                return pg

        return None

    def to_dict(self, id_parts=None):

        _d = dict(v=pbcommand.get_version(),
                  t=datetime.datetime.now().isoformat())

        d = BaseReportElement.to_dict(self, id_parts=id_parts)
        d['_comment'] = "Generated with pbcommand version {v} at {t}".format(**_d)

        # Required in 1.0.0 of the spec
        d['uuid'] = self.uuid
        d['title'] = self.title
        d['version'] = PB_REPORT_SCHEMA_VERSION
        d['dataset_uuids'] = list(set(self.dataset_uuids))
        return d

    def to_json(self):
        """Return a json string of the report"""

        from pbcommand.schemas import validate_pbreport

        try:
            s = _to_json_with_decoder(self.to_dict())
            # FIXME(mkocher)(2016-6-20) Enable schema validation
            # this needs to be processed by the decoder, then validate the
            # dict
            # _ = validate_pbreport(json.loads(s))
            return s
        except TypeError as e:
            msg = "Unable to serialize report due to {e} \n".format(e=e)
            log.error(msg)
            log.error("Object: " + pformat(self.to_dict()))
            raise

    def write_json(self, file_name):
        """
        Serialized the report to a json file.

        :param file_name: (str) Path to write output json file to.
        """
        with open(file_name, 'w') as f:
            f.write(self.to_json())
        # log.info("Wrote report {r}".format(r=file_name))

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
            attrs = OrderedDict()
            for ax in attributes_list:
                for a in ax:
                    if a.id in attrs:
                        attrs[a.id].append(a.value)
                    else:
                        attrs[a.id] = [a.value]
            return attrs

        def _merge_attributes_names(attributes_list):
            names = {}
            for ax in attributes_list:
                for a in ax:
                    if a.id in names:
                        assert names[a.id] == a.name
                    else:
                        names[a.id] = a.name
            return names

        def _attributes_to_table(attributes_list, table_id, title):
            attrs = _merge_attributes_d(attributes_list)
            labels = _merge_attributes_names(attributes_list)
            columns = [Column(k.lower(), header=labels[k], values=values)
                       for k, values in attrs.iteritems()]
            table = Table(table_id, title=title, columns=columns)
            return table

        def _sum_attributes(attributes_list):
            d = _merge_attributes_d(attributes_list)
            labels = _merge_attributes_names(attributes_list)
            return [Attribute(k, sum(values), name=labels[k])
                    for k, values in d.iteritems()]

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
        dataset_uuids = set()
        for report in reports:
            assert report.id == report_id
            attr_list.append(report.attributes)
            table_list.extend(report.tables)
            dataset_uuids.update(set(report.dataset_uuids))
        table = _attributes_to_table(attr_list, 'chunk_metrics',
                                     "Chunk Metrics")
        tables = _merge_tables(table_list)
        tables.append(table)
        merged_attributes = _sum_attributes(attr_list)
        return Report(report_id, attributes=merged_attributes, tables=tables,
                      dataset_uuids=sorted(list(dataset_uuids)))


########################################################################
# SPECIFICATION MODELS

FS_RE = "{([GMkp]{0,1})(:)([\.,]{0,1})([0-9]*)([dfg]{1})}(.*)$"
def format_metric(format_str, value):
    """
    Format a report metric (attribute or table column value) according to our
    in-house rules.  These resemble Python format strings (plus optional
    suffix), but with the addition of optional scaling flags.
    """
    if value is None:
        return "NA"
    elif format_str is None:
        return str(value)
    else:
        m = re.match(FS_RE, format_str)
        if m is None:
            raise ValueError("Format string '{s}' is uninterpretable".format(
                             s=format_str))
        if m.groups()[0] == 'p':
            value *= 100.0
        elif m.groups()[0] == 'G':
            value /= 1000000000.0
        elif m.groups()[0] == 'M':
            value /= 1000000.0
        elif m.groups()[0] == 'k':
            value /= 1000.0
        if isinstance(value, float) and m.groups()[4] == 'd':
            value = int(value)
        fs_python = "{{:{:s}{:s}{:s}}}".format(*(m.groups()[2:5]))
        formatted = fs_python.format(value)
        # the percent symbol can be implicit
        if m.groups()[0] == 'p' and m.groups()[-1] == '':
            return formatted + "%"
        else:
            return formatted + m.groups()[-1]


# FIXME this needs to be standardized
DATA_TYPES = {
    "int": int,
    "long": int,
    "float": float,
    "string": basestring, # this is hacky too
    "boolean": bool
}

class AttributeSpec(object):

    def __init__(self, id_, name, description, type_, format_):
        self.id = id_
        self.name = name
        self.description = description
        self._type = type_
        self.format_str = format_

    @property
    def type(self):
        return DATA_TYPES[self._type]

    @staticmethod
    def from_dict(d):
        return AttributeSpec(d['id'].split(".")[-1], d['name'],
                             d['description'], d["type"],
                             d.get("format", None))

    def validate_attribute(self, attr):
        assert attr.id == self.id
        if attr.value is not None and not isinstance(attr.value, self.type):
            raise TypeError("Attribute {i} has value of type {v} (expected {t})".format(i=self.id, v=type(attr.value).__name__, t=self.type))


class ColumnSpec(object):
    def __init__(self, id_, header, description, type_, format_):
        self.id = id_
        self.header = header
        self.description = description
        self._type = type_
        self.format_str = format

    @property
    def type(self):
        return DATA_TYPES[self._type]

    @staticmethod
    def from_dict(d):
        return ColumnSpec(d['id'].split(".")[-1], d['header'],
                          d['description'], d["type"], d.get("format", "%s"))

    def validate_column(self, col):
        assert col.id == self.id
        for value in col.values:
            if value is not None and not isinstance(value, self.type):
                raise TypeError("Column {i} contains value of type {v} (expected {t})".format(i=self.id, v=value_type, t=self.type))


class TableSpec(object):

    def __init__(self, id_, title, description, columns):
        self.id = id_
        self.title = title
        self.description = description
        self.columns = columns
        self._col_dict = {c.id: c for c in columns}

    @staticmethod
    def from_dict(d):
        return TableSpec(d['id'].split(".")[-1], d['title'], d['description'],
                         [ColumnSpec.from_dict(c) for c in d['columns']])

    def get_column_spec(self, id_):
        return self._col_dict.get(id_, None)


class PlotSpec(object):

    def __init__(self, id_, description, caption, title, xlabel, ylabel):
        self.id = id_
        self.description = description
        self.caption = caption
        self.title = title
        self.xlabel = xlabel
        self.ylabel = ylabel

    @staticmethod
    def from_dict(d):
        return PlotSpec(d['id'].split(".")[-1], d['description'],
                        d['caption'], d['title'],
                        d.get('xlabel', None), d.get('ylabel', None))


class PlotGroupSpec(object):

    def __init__(self, id_, title, description, legend, plots=()):
        self.id = id_
        self.title = title
        self.description = description
        self.legend = legend
        self.plots = plots
        self._plot_dict = {p.id: p for p in plots}

    @staticmethod
    def from_dict(d):
        return PlotGroupSpec(d['id'].split(".")[-1], d['title'],
                             d["description"], d['legend'],
                             [PlotSpec.from_dict(p) for p in d['plots']])


    def get_plot_spec(self, id_):
        return self._plot_dict.get(id_, None)


class ReportSpec(object):
    """
    Model for a specification of the expected content of a uniquely
    identified report.  For obvious reasons this mirrors the Report model,
    minus values and with added view metadata.  These specs should usually
    be written out explicitly in JSON rather than built programatically.
    """

    def __init__(self, id_, version, title, description, attributes=(),
                 plotgroups=(), tables=()):
        self.id = id_
        self.version = version
        self.title = title
        self.description = description
        self.attributes = attributes
        self.plotgroups = plotgroups
        self.tables = tables
        self._attr_dict = {a.id: a for a in attributes}
        self._plotgrp_dict = {p.id: p for p in plotgroups}
        self._table_dict = {t.id: t for t in tables}

    @staticmethod
    def from_dict(d):
        return ReportSpec(d['id'], d['version'], d['title'], d['description'],
                          [AttributeSpec.from_dict(a)
                           for a in d['attributes']],
                          [PlotGroupSpec.from_dict(p)
                           for p in d['plotGroups']],
                          [TableSpec.from_dict(t) for t in d['tables']])

    def get_attribute_spec(self, id_):
        return self._attr_dict.get(id_, None)

    def get_plotgroup_spec(self, id_):
        return self._plotgrp_dict.get(id_, None)

    def get_table_spec(self, id_):
        return self._table_dict.get(id_, None)

    def validate_report(self, rpt):
        """
        Check that a generated report corresponding to this spec is compliant
        with the expected types and object IDs.  (Missing objects will not
        result in an error, but unexpected object IDs will.)
        """
        assert rpt.id == self.id
        # TODO check version?
        errors = []
        for attr in rpt.attributes:
            attr_spec = self.get_attribute_spec(attr.id)
            if attr_spec is None:
                errors.append("Attribute {i} not found in spec".format(
                              i=attr.id))
            else:
                try:
                    attr_spec.validate_attribute(attr)
                except TypeError as e:
                    errors.append(str(e))
                try:
                    format_metric(attr_spec.format_str, attr.value)
                except (ValueError, TypeError) as e:
                    log.error(e)
                    errors.append("Couldn't format {i}: {e}".format(
                                  i=attr.id, e=str(e)))
        for table in rpt.tables:
            table_spec = self.get_table_spec(table.id)
            if table_spec is None:
                errors.append("Table {i} not found in spec".format(i=table.id))
            else:
                for column in table.columns:
                    column_spec = table_spec.get_column_spec(column.id)
                    if column_spec is None:
                        errors.append("Column {i} not found in spec".format(
                                      i=column.id))
                    else:
                        try:
                            column_spec.validate_column(column)
                        except TypeError as e:
                            errors.append(str(e))
        for pg in rpt.plotGroups:
            pg_spec = self.get_plotgroup_spec(pg.id)
            if pg_spec is None:
                errors.append("Plot group {i} not found in spec".format(
                              i=pg.id))
            else:
                for plot in pg.plots:
                    plot_spec = pg.get_plot_spec(plot.id)
                    if plot_spec is None:
                        errors.append("Plot {i} not found in spec".format(
                                      i=plot.id))
        if len(errors) > 0:
            raise ValueError(
                "Report {i} failed validation against spec:\n{e}".format(
                i=self.id, e="\n".join(errors)))
        return rpt

    def is_valid_report(self, rpt):
        try:
            rpt = self.validate_report(rpt)
            return True
        except ValueError:
            return False
