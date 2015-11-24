"""CLI for interacting with the PacBio Services

0.1.0 Version, Import/Convert datasets

pbservice import-dataset # dir or XML file
pbservice import-rs-movie # dir or XML file (Requires 'movie-to-dataset' exe)
pbservice import-ref-info # dir or XML file (Requires 'reference-to-dataset' exe)
pbservice import-fasta /path/to/file.fasta --name my-name --organism my-org --ploidy haploid

0.2.0 Version, Jobs Support, leveraging

pbservice run-analysis path/to/file.json
pbservice run-merge-dataset path/to/file.json


"""

import os
import sys
import logging
import functools
import time
import traceback

from pbcommand.cli import get_default_argparser, pacbio_args_runner
from pbcommand.services import ServiceAccessLayer
from pbcommand.utils import setup_log, compose
from pbcommand.validators import validate_file
from pbcommand.common_options import add_base_options
from pbcommand.utils import which_or_raise

__version__ = "0.1.0"

log = logging.getLogger(__name__)


class Constants(object):
    FASTA_TO_REFERENCE = "fasta-to-reference"
    RS_MOVIE_TO_DS = "movie-metadata-to-dataset"


def _is_xml(path):
    return path.endswith(".xml")


def validate_xml_file_or_dir(path):
    px = os.path.abspath(os.path.expanduser(path))
    if os.path.isdir(px):
        return px
    elif os.path.isfile(px) and _is_xml(px):
        return px
    else:
        raise ValueError("Expected dir or file '{p}'".format(p=path))


def add_sal_options(p):
    p.add_argument('--host', type=str,
                   default="http://localhost", help="Server host")
    p.add_argument('--port', type=int, default=8070, help="Server Port")
    return p


def add_xml_or_dir_option(p):
    p.add_argument('xml_or_dir', type=validate_xml_file_or_dir, help="Directory or XML file.")
    return p


def add_sal_and_xml_dir_options(p):
    fx = [add_base_options,
          add_sal_options,
          add_xml_or_dir_option]
    f = compose(*fx)
    return f(p)


def get_sal_and_status(host, port):
    """Get Sal or Raise if status isn't successful"""
    sal = ServiceAccessLayer(host, port)
    sal.get_status()
    return sal


def run_file_or_dir(file_func, dir_func, xml_or_dir):
    if os.path.isdir(xml_or_dir):
        return dir_func(xml_or_dir)
    elif os.path.isfile(xml_or_dir):
        return file_func(xml_or_dir)
    else:
        raise ValueError("Unsupported value {x}".format(x=xml_or_dir))


def walker(root_dir, file_filter_func):
    for root, dnames, fnames in os.walk(root_dir):
        for fname in fnames:
            path = os.path.join(root, fname)
            if file_filter_func(path):
                yield path


def is_dataset(path):
    """peek into the XML to get the MetaType"""
    # FIXME
    return False


def is_xml_dataset(path):
    if _is_xml(path):
        if is_dataset(path):
            return True
    return False


def dataset_walker(root_dir):
    filter_func = is_xml_dataset
    return walker(root_dir, filter_func)


def import_dataset(sal, dataset_meta_type, path):
    log.info("Mock {s} importing dataset type {t} path:{p}".format(t=dataset_meta_type, p=path, s=sal))
    # FIXME
    return True


def import_datasets(sal, root_dir):
    for path in dataset_walker(root_dir):
        meta_type = ""
        result = import_dataset(sal, meta_type, path)
        yield result


def run_import_datasets(host, port, xml_or_dir):
    sal = ServiceAccessLayer(host, port)
    file_func = functools.partial(import_dataset, sal)
    dir_func = functools.partial(import_datasets, sal)
    return run_file_or_dir(file_func, dir_func, xml_or_dir)


def args_runner_import_datasets(args):
    return run_import_datasets(args.host, args.port, args.xml_or_dir)


def is_movie_metadata(path):
    """Peek into XML to see if it's a movie metadata XML file"""
    # FIXME
    return False


def is_xml_movie_metadata(path):
    if _is_xml(path):
        if is_movie_metadata(path):
            return True
    return False


def movie_metadata_walker(root_dir):
    f = is_movie_metadata
    return walker(root_dir, f)


def import_rs_movie(sal, path):
    # FIXME
    log.info("Mock RS movie-metadata XML {p}".format(p=path))
    return True


def import_rs_movies(sal, root_dir):
    for path in movie_metadata_walker(root_dir):
        result = import_rs_movie(sal, path)
        yield result


def run_import_rs_movies(host, port, xml_or_dir):
    which_or_raise(Constants.RS_MOVIE_TO_DS)
    sal = ServiceAccessLayer(host, port)
    file_func = functools.partial(import_rs_movie, sal)
    dir_func = functools.partial(import_rs_movies, sal)
    return run_file_or_dir(file_func, dir_func, xml_or_dir)


def args_runner_import_rs_movies(args):
    return run_import_rs_movies(args.host, args.port, args.xml_or_dir)


def add_import_fasta_opts(p):
    px = p.add_argument
    px('fasta_path', type=validate_file, help="Path to Fasta File")
    px('--name', required=True, type=str, help="Name of ReferenceSet")
    px('--organism', required=True, type=str, help="Organism")
    px('--ploidy', required=True, type=str, help="Ploidy")
    px('--block', type=bool, default=False, help="Block during importing process")
    add_sal_options(p)
    add_base_options(p)
    return p


def run_import_fasta(host, port, fasta_path, name, organism, ploidy, block=False):
    sal = ServiceAccessLayer(host, port)
    if block is True:
        sal.run_import_fasta(fasta_path, name, organism, ploidy)
    else:
        sal.import_fasta(fasta_path, name, organism, ploidy)

    return 0


def args_run_import_fasta(args):
    log.debug(args)
    return run_import_fasta(args.host, args.port, args.fasta_path,
                            args.name, args.organism, args.ploidy, block=args.block)


def subparser_builder(subparser, subparser_id, description, options_func, exe_func):
    """
    Util to add subparser options

    :param subparser:
    :param subparser_id:
    :param description:
    :param options_func: Function that will add args and options to Parser instance F(subparser) -> None
    :param exe_func: Function to run F(args) -> Int
    :return:
    """
    p = subparser.add_parser(subparser_id, help=description)
    options_func(p)
    p.set_defaults(func=exe_func)
    return p


def get_parser():
    desc = "Tool to import datasets, convert/import fasta file and run analysis jobs"
    p = get_default_argparser(__version__, desc)

    sp = p.add_subparsers(help='commands')

    def builder(subparser_id, description, options_func, exe_func):
        subparser_builder(sp, subparser_id, description, options_func, exe_func)

    ds_desc = "Import DataSet XML "
    builder('import-dataset', ds_desc,
            add_sal_and_xml_dir_options, args_runner_import_datasets)

    rs_desc = "Import RS Metadata XML"
    builder("import-rs-movie", rs_desc,
            add_sal_and_xml_dir_options, args_runner_import_rs_movies)

    fasta_desc = "Import Fasta (and convert to ReferenceSet)"
    builder("import-fasta", fasta_desc, add_import_fasta_opts, args_run_import_fasta)

    return p


def args_executer(args):
    """
    This is pulled from pbsmrtpipe that uses the goofy func=my_runner_func,
    which will be called using args.func(args)

    :rtype int
    """
    try:
        return_code = args.func(args)
    except Exception as e:
        log.error(e, exc_info=True)
        traceback.print_exc(sys.stderr)
        if isinstance(e, IOError):
            return_code = 1
        else:
            return_code = 2

    return return_code


def main_runner(argv, parser, exe_runner_func, setup_log_func, alog):
    """
    Fundamental interface to commandline applications
    """
    started_at = time.time()
    args = parser.parse_args(argv)
    # log.debug(args)

    # setup log
    if hasattr(args, 'debug'):
        if args.debug:
            setup_log_func(alog, level=logging.DEBUG)
        else:
            alog.addHandler(logging.NullHandler())
    else:
        alog.addHandler(logging.NullHandler())

    log.debug(args)
    alog.info("Starting tool version {v}".format(v=parser.version))
    rcode = exe_runner_func(args)

    run_time = time.time() - started_at
    _d = dict(r=rcode, s=run_time)
    alog.info("exiting with return code {r} in {s:.2f} sec.".format(**_d))
    return rcode


def main(argv=None):

    argv_ = sys.argv if argv is None else argv
    parser = get_parser()

    return pacbio_args_runner(argv_[1:], parser, args_executer, setup_log, log)
