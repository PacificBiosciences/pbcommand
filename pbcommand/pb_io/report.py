"""Loading a report from JSON

This manual marshalling/de-marshalling is not awesome.
"""
import json
import logging

from pbcommand.models.report import (Report, Plot, PlotGroup, Attribute,
                                     Table, Column)

SUPPORTED_VERSIONS = ('2.1', '2.2', '2.3')
_DEFAULT_VERSION = '2.1'  # before the version was officially added

log = logging.getLogger(__name__)

__all__ = ["load_report_from_json"]


def _to_id(s):
    if '.' in s:
        return s.split('.')[-1]
    else:
        return s


def _to_plot(d):
    id_ = _to_id(d['id'])
    caption = d.get('caption', None)
    image = d['image']
    thumbnail = d.get('thumbnail', None)
    p = Plot(id_, image, caption=caption, thumbnail=thumbnail)
    return p


def _to_plot_group(d):
    id_ = _to_id(d['id'])
    legend = d.get('legend', None)
    thumbnail = d.get('thumbnail', None)
    # is this optional?
    title = d.get('title', None)

    if 'plots' in d:
        plots = [_to_plot(pd) for pd in d['plots']]
    else:
        plots = []

    return PlotGroup(id_, title=title, legend=legend, plots=plots,
                     thumbnail=thumbnail)


def _to_attribute(d):
    id_ = _to_id(d['id'])
    name = d.get('name', None)
    # this can't be none
    value = d['value']
    return Attribute(id_, value, name=name)


def _to_column(d):
    id_ = _to_id(d['id'])
    header = d.get('header', None)
    values = d.get('values', [])
    return Column(id_, header=header, values=values)


def _to_table(d):
    id_ = _to_id(d['id'])
    title = d.get('title', None)

    columns = []
    for column_d in d.get('columns', []):
        c = _to_column(column_d)
        columns.append(c)

    # assert that all the columns have the same number of values
    nvalues = {len(c.values) for c in columns}
    assert len(nvalues) == 1

    return Table(id_, title=title, columns=columns)


def dict_to_report(dct):
    if '_version' in dct:
        version = dct['_version']
        if version not in SUPPORTED_VERSIONS:
            # should this raise an exception?
            log.warn("{v} is an unsupported version. Supported versions {vs}".format(v=version, vs=SUPPORTED_VERSIONS))

    report_id = dct['id']

    plot_groups = []
    if 'plotGroups' in dct:
        pg = dct['plotGroups']
        if pg:
            plot_groups = [_to_plot_group(d) for d in pg]

    attributes = []
    for r_attr in dct.get('attributes', []):
        attr = _to_attribute(r_attr)
        attributes.append(attr)

    tables = []
    for table_d in dct.get('tables', []):
        t = _to_table(table_d)
        tables.append(t)

    report = Report(report_id, plotgroups=plot_groups, tables=tables,
                    attributes=attributes)

    return report


def load_report_from_json(json_file):
    """Convert a report json file to Report instance."""

    with open(json_file, 'r') as f:
        d = json.loads(f.read())
    r = dict_to_report(d)
    return r
