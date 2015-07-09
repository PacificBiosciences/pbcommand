import os
import logging
import functools
import subprocess


log = logging.getLogger(__name__)


def trigger_nfs_refresh(ff):
    """
    Central place for all NFS hackery

    Return whether a file or a dir ff exists or not.
    Call ls instead of python os.path.exists to eliminate NFS errors.

    Added try/catch black hole exception cases to help trigger an NFS refresh

    :rtype bool:

    # Yuan Li and various people contributed.
    """
    # try to trigger refresh for File case
    try:
        f = open(ff, 'r')
        f.close()
    except Exception:
        pass

    # try to trigger refresh for Directory case
    try:
        _ = os.stat(ff)
        _ = os.listdir(ff)
    except Exception:
        pass

    # Call externally
    cmd = "ls %s" % ff
    _, rcode, _ = subprocess.check_call(cmd)

    return rcode == 0


def _trigger_nfs_refresh_and_ignore(ff):
    """

    :rtype str
    """
    _ = trigger_nfs_refresh(ff)
    return ff


def _validate_resource(func, resource):
    """Validate the existence of a file/dir"""
    # Attempt to trigger an NFS metadata refresh
    _ = trigger_nfs_refresh(resource)

    if func(resource):
        return os.path.abspath(resource)
    else:
        raise IOError("Unable to find {f}".format(f=resource))


validate_file = functools.partial(_validate_resource, os.path.isfile)
validate_dir = functools.partial(_validate_resource, os.path.isdir)
validate_output_dir = functools.partial(_validate_resource, os.path.isdir)


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
    _ = trigger_nfs_refresh(fofn)

    if os.path.isfile(fofn):
        file_names = fofn_to_files(os.path.abspath(fofn))
        log.debug("Found {n} files in FOFN {f}.".format(n=len(file_names), f=fofn))
        return os.path.abspath(fofn)
    else:
        raise IOError("Unable to find {f}".format(f=fofn))


def fofn_to_files(fofn):
    """Util func to convert a bas/bax fofn file to a list of bas/bax files."""

    _ = trigger_nfs_refresh(fofn)

    if os.path.exists(fofn):
        with open(fofn, 'r') as f:
            bas_files = {line.strip() for line in f.readlines()}

        for bas_file in bas_files:
            if not os.path.isfile(bas_file):
                # try one more time to find the file by
                # performing an NFS refresh
                found = trigger_nfs_refresh(bas_file)
                if not found:
                    raise IOError("Unable to find bas/bax file '{f}'".format(f=bas_file))

        return list(bas_files)
    else:
        raise IOError("Unable to find FOFN {f}".format(f=fofn))
