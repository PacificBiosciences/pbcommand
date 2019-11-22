"""
Loading a report from JSON

This manual marshalling/de-marshalling is not awesome.
"""

import json
import logging
import uuid as U

from pbcommand.models.report import (Report, Plot, PlotGroup, Attribute,
                                     Table, Column, ReportSpec, PlotlyPlot)
from pbcommand.schemas import validate_report, validate_report_spec


log = logging.getLogger(__name__)

__all__ = [
    "load_report_from_json",
    "load_report_from",
    "load_report_spec_from_json",
]


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
    title = d.get('title', None)
    plot_type = d.get("plotType", Plot.PLOT_TYPE)
    plotly_version = d.get("plotlyVersion", None)
    if plot_type == Plot.PLOT_TYPE:
        return Plot(id_, image, caption=caption,
                    thumbnail=thumbnail, title=title)
    elif plot_type == PlotlyPlot.PLOT_TYPE:
        return PlotlyPlot(id_, image, caption=caption, thumbnail=thumbnail,
                          title=title, plotly_version=plotly_version)
    else:
        raise ValueError("Unrecognized plotType '{t}'".format(t=plot_type))


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
    # Use `load_report_from` instead.
    # FIXME. Add support for different version schemas in a cleaner, more
    # concrete manner.

    report_id = dct['id']

    # Make this optional for now
    report_uuid = dct.get('uuid', str(U.uuid4()))

    tags = dct.get('tags', [])

    # Make sure the UUID is well formed
    _ = U.UUID(report_uuid)

    # Legacy Reports > 0.3.9 will not have the title key
    title = dct.get('title', "Report {i}".format(i=report_id))

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

    report = Report(report_id,
                    title=title,
                    plotgroups=plot_groups,
                    tables=tables,
                    attributes=attributes,
                    dataset_uuids=dct.get('dataset_uuids', ()),
                    uuid=report_uuid, tags=tags)

    return report


def __load_json_or_dict(processor_func):
    def wrapper(json_path_or_dict):
        if isinstance(json_path_or_dict, dict):
            return processor_func(json_path_or_dict)
        else:
            with open(json_path_or_dict, 'r') as f:
                d = json.loads(f.read())
            return processor_func(d)
    return wrapper


def load_report_from(json_path_or_dict):
    """
    Load a Report from a raw dict or path to JSON file

    :param json_path_or_dict:
    :type json_path_or_dict: dict | str
    :return:
    """
    return __load_json_or_dict(dict_to_report)(json_path_or_dict)


def load_report_from_json(json_file):
    """Convert a report json file to Report instance."""
    # This should go way in favor of `load_report_from`
    return load_report_from(json_file)


def _to_report(nfiles, attribute_id, report_id):
    # this should have version of the bax/bas files, chemistry
    attributes = [Attribute(attribute_id, nfiles)]
    return Report(report_id, attributes=attributes)


def fofn_to_report(nfofns):
    return _to_report(nfofns, "nfofns", "fofn_report")


def load_report_spec_from_json(json_file, validate=True):
    with open(json_file, 'r') as f:
        d = json.loads(f.read())
        if validate:
            validate_report_spec(d)
        return ReportSpec.from_dict(d)
