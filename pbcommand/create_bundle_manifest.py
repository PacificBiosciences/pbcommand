#!/usr/bin/env python
# Tool to generate the manifest.xml will the correct datetime of bundle
# creation as well as add git sha and bamboo build metadata

import sys
import os
import argparse
import json
import subprocess
import datetime

from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

__version__ = "0.3.0"


class Constants:
    # there's an Id versus name issue here
    NAME = "SMRT Link Software Update"
    AUTHOR = "build"


def get_parser():
    desc = "Update the manifest.xml"
    p = argparse.ArgumentParser(description=desc,
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("bundle_id", help="ID of the software bundle")
    p.add_argument("version_txt", help="Path to Manifest.xml")
    p.add_argument("-o", dest="output_manifest_xml", default="manifest.xml",
                   help="Path to the version.txt file. Must have a single line with Major.Minor.Tiny format")
    p.add_argument("-j", dest="pacbio_manifest_json", default="pacbio-manifest.json", help="Output path output manifest JSON used by SMRT Link")
    p.add_argument('--author', dest="author", default=Constants.AUTHOR, help="Bundle creation Author")
    p.add_argument("--name", dest="name", default=Constants.NAME,
                   help="Name of software bundle")
    p.add_argument("--desc", dest="description", default="No Description",
                   help="Description to appear in the manifest")
    return p


def git_short_sha():
    args = "git rev-parse --short HEAD".split()
    return subprocess.check_output(args).strip()


def git_branch():
    args = "git rev-parse --abbrev-ref HEAD".split()
    return subprocess.check_output(args).strip()


def get_bamboo_buildnumber(default=0):
    return int(os.environ.get('bamboo_globalBuildNumber', default))


def to_semver(major, minor, patch, git_sha, build_number=None, prerelease_tag=None):
    """Convert to semver format"""
    base = ".".join(str(i) for i in (major, minor, patch))
    prerelease = "" if prerelease_tag is None else "-{}".format(prerelease_tag)
    # need the trailing . as the sep for the other metadata
    number = "" if build_number is None else str(build_number) + "."
    metadata = "+{b}{s}".format(b=number, s=git_sha[:7])
    return "{b}{p}{m}".format(b=base, p=prerelease, m=metadata)


def to_undocumented_pacbio_version_format(major, minor, tiny, other):
    return ".".join([str(i) for i in (major, minor, tiny, other)])


def get_version(major, minor, tiny):
    build_number = get_bamboo_buildnumber()
    git_sha = git_short_sha()
    # The build number is being abused for the patch version
    return to_semver(major, minor, tiny, git_sha, build_number=None)


def read_version_txt(path):
    with open(path, 'r') as f:
        x = f.readline()

    major, minor, patch = [int(i) for i in x.split(".")][:3]
    return major, minor, patch


def to_pacbio_manifest_d(bundle_id, version, name, desc):
    return dict(id=bundle_id,
                name=name,
                version=version,
                description=desc,
                dependencies=[])


def prettify(elem):
    return minidom.parseString(tostring(elem, 'utf-8')).toprettyxml(indent="  ")


def write_manifest_xml(bundle_id, version, name, description, author, manifest_xml):

    root = Element("Manifest")

    def sub(n, value_):
        e = SubElement(root, n)
        e.text = value_
        return e

    sub("Package", bundle_id)
    sub("Name", name)
    sub("Version", version)
    sub("Created", datetime.datetime.utcnow().isoformat())
    sub("Author", author)
    sub("Description", description)

    with open(manifest_xml, 'w') as f:
        f.write(prettify(root))

    return root


def write_pacbio_manifest_json(bundle_id, version, name, desc, output_json):
    with open(output_json, 'w') as f:
        f.write(json.dumps(to_pacbio_manifest_d(bundle_id, version, name, desc), indent=True))


def runner(bundle_id, version_txt, output_manifest_xml, pacbio_manifest_json, author, name, desc):

    major, minor, patch = read_version_txt(version_txt)
    sem_ver = get_version(major, minor, patch)
    branch = git_branch()
    other = get_bamboo_buildnumber()
    if not (branch.startswith("master") or branch.startswith("release")):
        other = "SNAPSHOT" + str(other)
    version_str = to_undocumented_pacbio_version_format(major, minor, patch, other)

    # this is to get the git SHA1 and build number propagated to SL services data model
    author = "User {} created {} bundle {}".format(Constants.AUTHOR, bundle_id, sem_ver)

    # there's some tragic duplication of models between ICS and Secondary
    # hence this duplication of these very similar ideas
    write_manifest_xml(bundle_id, version_str, name, desc, author, output_manifest_xml)
    write_pacbio_manifest_json(bundle_id, version_str, name, desc, pacbio_manifest_json)

    return 0


def main(argv_):
    p = get_parser()
    args = p.parse_args(argv_)
    return runner(args.bundle_id,
                  args.version_txt,
                  args.output_manifest_xml,
                  args.pacbio_manifest_json,
                  args.author,
                  args.name,
                  args.description)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
