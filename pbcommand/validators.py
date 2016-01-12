import os
import logging
import functools
from pbcommand.utils import nfs_exists_check

log = logging.getLogger(__name__)


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
    @functools.wraps
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


def validate_report(report_file_name):
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
