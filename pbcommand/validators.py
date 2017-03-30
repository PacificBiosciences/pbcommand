import os
import logging
import functools

from pbcommand.utils import nfs_exists_check
from pbcommand.pb_io import load_report_from_json

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler()) # squash annoying no Handler msg


def trigger_nfs_refresh(ff):
    # keeping this for backward compatibility
    return nfs_exists_check(ff)


def _validate_resource(func, resource):
    """Validate the existence of a file/dir"""
    _ = nfs_exists_check(resource)

    if func(resource):
        return os.path.abspath(resource)
    else:
        raise IOError("Unable to find '{f}'".format(f=resource))


validate_file = functools.partial(_validate_resource, os.path.isfile)
validate_dir = functools.partial(_validate_resource, os.path.isdir)
validate_output_dir = functools.partial(_validate_resource, os.path.isdir)


def validate_or(f1, f2, error_msg):
    """
    Apply Valid functions f1, then f2 (if failure occurs)

    :param error_msg: Default message to print
    """
    def wrapper(path):
        try:
            return f1(path)
        except Exception:
            try:
                return f2(path)
            except Exception as e:
                log.error("{m} {p} \n. {e}".format(m=error_msg, p=path, e=repr(e)))
                raise

    return wrapper


def validate_report_file(report_file_name):
    """
    Raise ValueError if report contains path seps
    """
    if not os.path.basename(report_file_name) == report_file_name:
        raise ValueError("Path separators are not allowed: {r}".format(r=report_file_name))
    return report_file_name


def validate_fofn(fofn):
    """Validate existence of FOFN and files within the FOFN.

    :param fofn: (str) Path to File of file names.
    :raises: IOError if any file is not found.
    :return: (str) abspath of the input fofn
    """
    _ = nfs_exists_check(fofn)

    if os.path.isfile(fofn):
        file_names = fofn_to_files(os.path.abspath(fofn))
        log.debug("Found {n} files in FOFN {f}.".format(n=len(file_names), f=fofn))
        return os.path.abspath(fofn)
    else:
        raise IOError("Unable to find {f}".format(f=fofn))


def validate_nonempty_file(resource):
    try:
        resource_path = validate_file(resource)
    except IOError as e:
        raise e
    try:
        with open(resource_path) as handle:
            l = [handle.next() for i in range(2)]
    except StopIteration:
        raise IOError("{f} appears to be empty".format(f=resource))
    return resource_path


def fofn_to_files(fofn):
    """Util func to convert a bas/bax fofn file to a list of bas/bax files."""

    _ = nfs_exists_check(fofn)

    if os.path.exists(fofn):
        with open(fofn, 'r') as f:
            bas_files = {line.strip() for line in f.readlines()}

        for bas_file in bas_files:
            if not os.path.isfile(bas_file):
                # try one more time to find the file by
                # performing an NFS refresh
                found = nfs_exists_check(bas_file)
                if not found:
                    raise IOError("Unable to find bas/bax file '{f}'".format(f=bas_file))

        return list(bas_files)
    else:
        raise IOError("Unable to find FOFN {f}".format(f=fofn))


def validate_report(file_name):
    e = []
    base_path = os.path.dirname(file_name)
    r = load_report_from_json(file_name)
    if r.title is None:
        e.append("Report {i} is missing a title".format(i=r.id))
    for t in r.tables:
        if t.title is None:
            e.append("Table {r}.{t} is missing a title".format(r=r.id, t=t.id))
        for col in t.columns:
            if col.header is None:
                e.append("Column {r}.{t}.{c} is missing a header".format(
                         r=r.id, t=t.id, c=col.id))
        lengths = set([len(col.values) for col in t.columns])
        if len(lengths) != 1:
            e.append("Inconsistent column sizes in table {r}.{t}: {s}".format(
                     r=r.id, t=t.id, s=",".join(
                     [str(x) for x in sorted(list(lengths))])))
    for pg in r.plotGroups:
        if pg.title is None:
            e.append("Plot group {r}.{g} is missing a title".format(
                     r=r.id, g=pg.id))
        for plot in pg.plots:
            #if plot.caption is None:
            #    raise ValueError("Plot {r.g.p} is missing a caption".format(
            #                     r=r.id, g=pg.id, p=plot.id))
            if plot.image is None:
                e.append("Plot {r}.{g}.{p} does not have an image".format(
                         r=r.id, g=pg.id, p=plot.id))
            img_path = os.path.join(base_path, plot.image)
            if not os.path.exists(img_path):
                e.append("The plot image {f} does not exist".format(f=img_path))
            if plot.thumbnail is None:
                pass
                #raise ValueError("Plot {r.g.p} does not have an thumbnail image".format(
                #                 r=r.id, g=pg.id, p=plot.id))
            else:
                thumbnail = os.path.join(base_path, plot.thumbnail)
                if not os.path.exists(thumbnail):
                    e.append("The thumbnail image {f} does not exist".format(f=img_path))
        if pg.thumbnail is not None:
            thumbnail = os.path.join(base_path, pg.thumbnail)
            if not os.path.exists(thumbnail):
                e.append("The thumbnail image {f} does not exist".format(f=img_path))
    if len(e) > 0:
        raise ValueError("\n".join(e))
    return r
