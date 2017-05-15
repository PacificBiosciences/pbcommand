"""Example to show how to expose a subset of functionality to tool contract,
while exposing all the options via argparse commandline interface

In this example, the tool contract has

ins = (csv,)
outs = (report, )
options = alpha

In the "full" argpase layer that has an optional hdf5 option file and beta. (Option[Int] is the scala-style notation)

ins = (csv, )
outs (report, Option[h5])
options = alpha, beta
"""
import sys
import logging

from pbcommand.models import FileTypes, get_pbparser
from pbcommand.cli import pbparser_runner
from pbcommand.utils import setup_log

log = logging.getLogger(__name__)

TOOL_ID = "pbcommand.tasks.dev_mixed_app"
__version__ = "0.2.0"


def _get_contract_parser():
    """
    Central point of programmatically defining a Parser.
    :rtype: PbParser
    :return: PbParser
    """
    # Number of processors to use
    nproc = 2
    # Commandline exe to call "{exe}" /path/to/resolved-tool-contract.json
    driver_exe = "python -m pbcommand.cli.examples.dev_mixed_app --resolved-tool-contract "
    desc = "Dev app for Testing that supports emitting tool contracts"
    p = get_pbparser(TOOL_ID, __version__, "DevApp", desc, driver_exe,
                     is_distributed=False, nproc=nproc)
    return p


def add_rtc_options(p):
    """
    Add all ins/outs and options that will be in both the tool contract and the argparse layer

    :param p:
    :type p: pbcommand.models.PbParser
    :return:
    """
    p.add_input_file_type(FileTypes.CSV, "csv", "Input CSV", "Input csv description")
    p.add_output_file_type(FileTypes.REPORT, "rpt", "Output Report", "Output PacBio Report JSON", "example.report")
    p.add_int("pbcommand.task_options.alpha", "alpha", 25, "Alpha", "Alpha description")
    p.add_float("pbcommand.task_options.beta", "beta", 1.234, "Beta", "Beta description")
    p.add_boolean("pbcommand.task_options.gamma", "gamma", True, "Gamma", "Gamma description")
    p.add_choice_str("pbcommand.task_options.ploidy", "ploidy", ["haploid", "diploid"], "Ploidy", "Genome ploidy", "haploid")
    p.add_choice_int("pbcommand.task_options.delta", "delta", [1, 2, 3], "Delta", "An integer choice", default=1)
    p.add_choice_float("pbcommand.task_options.epsilon", "epsilon", [0.01, 0.1, 1.0], "Epsilon", "A float choice", default=0.1)
    p.add_str("pbcommand.task_options.comment", "comment", "asdf", "Comments", "A string parameter")
    return p


def add_argparse_only(p):
    """
    Standard argparse layer

    :param p:
    :type p: argparse.ArgumentParser
    :return:
    """
    p.add_argument("--output-h5", type=str, help="Optional output H5 file.")
    p.add_argument("--zeta", type=int, default=1234, help="Example option")
    return p


def get_contract_parser():
    p = _get_contract_parser()
    # minimal ins/outs + options exposed at the contract level
    add_rtc_options(p)
    # add all options to the raw argparse instance
    add_argparse_only(p.arg_parser.parser)
    return p


def _fake_main(csv, report_json, alpha=1, beta=1.234, gamma=True, delta=1, epsilon=1234,
               output_h5=None, ploidy=None, zeta=None):
    _d = dict(c=csv, r=report_json, a=alpha, b=beta, g=gamma, d=delta, e=epsilon, h=output_h5, p=ploidy)
    log.info("Running main with {c} {r} alpha={a} beta={b} gamma={g} delta={d} epsilon={e} h5={h} p={p}".format(**_d))
    with open(report_json, "w") as f:
        f.write("{}")
    return 0


def args_runner(args):
    """Standard python args access point"""

    csv = args.csv
    report_json = args.rpt
    output_h5 = args.output_h5
    return _fake_main(csv, report_json, alpha=args.alpha, beta=args.beta, gamma=args.gamma, epsilon=args.epsilon, output_h5=output_h5, ploidy=args.ploidy, zeta=args.zeta)


def resolved_tool_contract_runner(rtc):
    """

    :param rtc:
    :type rtc: pbcommand.models.tool_contract.ResolvedToolContract
    :return:
    """
    csv = rtc.task.input_files[0]
    rpt = rtc.task.output_files[0]
    alpha = rtc.task.options["pbcommand.task_options.alpha"]
    beta = rtc.task.options["pbcommand.task_options.beta"]
    gamma = rtc.task.options["pbcommand.task_options.gamma"]
    ploidy = rtc.task.options["pbcommand.task_options.ploidy"]
    delta = rtc.task.options["pbcommand.task_options.delta"]
    epsilon = rtc.task.options["pbcommand.task_options.epsilon"]
    comments = rtc.task.options["pbcommand.task_options.comment"]
    return _fake_main(csv, rpt, alpha=alpha, beta=beta, gamma=gamma,
                      ploidy=ploidy)


def main(argv=sys.argv):
    # New interface that supports running resolved tool contracts
    log.info("Starting {f} version {v} pbcommand example dev app".format(f=__file__, v=__version__))

    p = get_contract_parser()
    return pbparser_runner(argv[1:], p,
                           args_runner,
                           resolved_tool_contract_runner,
                           log,
                           setup_log)


if __name__ == '__main__':
    sys.exit(main())
