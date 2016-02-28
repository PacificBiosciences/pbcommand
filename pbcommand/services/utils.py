# This is not public. Might want to move this into service_access_layer
from collections import defaultdict

from .models import ServiceJob, JobStates, JobTypes


def to_ascii(s):
    # This is not awesome
    return s.encode('ascii', 'ignore')


def _jobs_by_state_gen(sal, job_states):
    """:type sal: ServiceAccessLayer"""

    states = job_states if isinstance(job_states, (tuple, list)) else [job_states]

    jobs = sal.get_analysis_jobs()
    for job in jobs:
        sjob = ServiceJob.from_d(job)
        if sjob.state in states:
            yield sjob


def get_failed_jobs(sal):
    return sorted(_jobs_by_state_gen(sal, JobStates.FAILED), key=lambda x: x.created_at, reverse=True)


def jobs_summary(jobs):
    """dict(state) -> count (int) """
    states_counts = defaultdict(lambda: 0)
    if jobs:
        for job in jobs:
            states_counts[job.state] += 1

    return states_counts


def to_jobs_summary(jobs, header=None):
    """Return string of jobs summary"""
    header = "Jobs" if header is None else header

    # Make easier to handle Option[Seq[Job]]
    xjobs = [] if jobs is None else jobs

    outs = []
    x = outs.append
    states_counts = jobs_summary(xjobs)
    x("{h} {n}".format(n=len(xjobs), h=header))
    for state, c in states_counts.iteritems():
        x("State {s} {c}".format(c=c, s=state))

    return "\n".join(outs)


def to_all_job_types_summary(sal, sep="*****"):

    # only  use a subset of the job types

    funcs = [(JobTypes.IMPORT_DS, sal.get_import_dataset_jobs),
             (JobTypes.MERGE_DS, sal.get_merge_dataset_jobs),
             (JobTypes.CONVERT_FASTA, sal.get_fasta_convert_jobs),
             (JobTypes.PB_PIPE, sal.get_analysis_jobs)]

    outs = []
    x = outs.append
    x("All Job types Summary")
    x(sep)
    for name, func in funcs:
        out = to_jobs_summary(func(), header="{n} Jobs".format(n=name))
        x(out)
        x(sep)

    return "\n".join(outs)


def to_all_datasets_summary(sal, sep="****"):

    ds_types = [("SubreadSets", sal.get_subreadsets),
                ("HdfSubreadSets", sal.get_hdfsubreadsets),
                ("ReferenceSets", sal.get_referencesets),
                ("AlignmentSets", sal.get_alignmentsets),
                #("ConsensusSets", sal.get_ccsreadsets)
                ]

    outs = []
    x = outs.append
    x("Dataset Summary")
    x(sep)
    for name, func in ds_types:
        d = func()
        ndatasets = len(d)
        x("{n} {d}".format(n=name, d=ndatasets))

    return "\n".join(outs)


def to_sal_summary(sal):
    """:type sal: ServiceAccessLayer"""

    status = sal.get_status()
    outs = []

    x = outs.append

    sep = "-" * 10

    x(repr(sal))
    x("Status {s}".format(s=status['message']))
    x(sep)
    x(to_all_datasets_summary(sal, sep=sep))
    x(sep)
    x(to_all_job_types_summary(sal, sep=sep))

    return "\n".join(outs)
