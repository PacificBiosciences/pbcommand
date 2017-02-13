#!/usr/bin/env python

"""
Generate C++ preprocessor defines for PacBio file type IDs.
"""

import argparse
import sys

from pbcommand.models import FileType, FileTypes


def run(argv):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("out", nargs='?', default="FileTypes.h")
    args = p.parse_args(argv[1:])
    with open(args.out, "w") as out:
        for ft_name in dir(FileTypes):
            ft = getattr(FileTypes, ft_name)
            if isinstance(ft, FileType):
                out.write("""#define {f} "{i}"\n""".format(f=ft_name, i=ft.file_type_id))
    print "Wrote {f}".format(f=args.out)
    return 0


if __name__ == "__main__":
    sys.exit(run(sys.argv))
