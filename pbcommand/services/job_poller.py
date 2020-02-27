#!/usr/bin/env python3

"""
SMRT Link job poller with retry mechanism
"""

import logging
import time
import json
import sys

from requests import RequestException

from pbcommand.cli.core import get_default_argparser_with_base_opts, pacbio_args_runner
from pbcommand.services._service_access_layer import (get_smrtlink_client,
                                                      _to_url,
                                                      _process_rget)
from pbcommand.services.models import add_smrtlink_server_args, JobExeError, JobStates, ServiceJob
from pbcommand.utils import setup_log

__version__ = "0.1"
log = logging.getLogger(__name__)


def poll_for_job_completion(job_id,
                            host,
                            port,
                            user,
                            password,
                            time_out=None,
                            sleep_time=60,
                            retry_on=(),
                            abort_on_interrupt=False):
    def _get_client():
        return get_smrtlink_client(host, port, user, password)
    started_at = time.time()
    retry_time = sleep_time
    auth_errors = 0
    external_job_id = None
    LOG_INTERVAL = 600
    i = 0
    try:
        client = _get_client()
        while True:
            i += 1
            job_uri = "/smrt-link/job-manager/jobs/analysis/{}".format(job_id)
            url = _to_url(client.uri, job_uri)
            try:
                job_json = _process_rget(url, headers=client._get_headers())
            except RequestException as e:
                if e.errno == 401:
                    auth_errors += 1
                    if auth_errors > 10:
                        raise RuntimeError("10 successive HTTP 401 errors, exiting")
                    log.warn("Authentication error, will retry with new token")
                    client = _get_client()
                    continue
                elif e.errno in retry_on:
                    log.warn("Got HTTP {c}, will retry in {d}s".format(
                        c=e.errno, d=retry_time))
                    time.sleep(retry_time)
                    # if a retryable error occurs, we increment the retry time
                    # up to a max of 30 minutes
                    retry_time = max(1800, retry_time + sleep_time)
                    continue
                else:
                    raise
            else:
                # if request succeeded, reset the retry_time
                auth_errors = 0
                retry_time = sleep_time
                run_time = time.time() - started_at
                job = ServiceJob.from_d(job_json)
                if external_job_id is None and job.external_job_id is not None:
                    external_job_id = job.external_job_id
                    log.info("Cromwell workflow ID is %s", external_job_id)
                if job.state in JobStates.ALL_COMPLETED or sleep_time == 0:
                    return job_json
                msg = "Running pipeline {n} (job {j}) state: {s} runtime:{r:.2f} sec {i} iteration".format(
                    n=job.name, j=job.id, s=job.state, r=run_time, i=i)
                if run_time % LOG_INTERVAL < sleep_time:
                    log.info(msg)
                else:
                    log.debug(msg)
                if time_out is not None:
                    if run_time > time_out:
                        raise JobExeError(
                            "Exceeded runtime {r} of {t}".format(
                                r=run_time, t=time_out))
                time.sleep(sleep_time)
    except KeyboardInterrupt:
        if abort_on_interrupt:
            client.terminate_job_id(job_id)
        raise


def run_args(args):
    d = poll_for_job_completion(
        args.job_id,
        args.host,
        args.port,
        args.user,
        args.password,
        time_out=args.max_time,
        sleep_time=args.poll_interval,
        retry_on=args.retry_on,
        abort_on_interrupt=False)
    job = ServiceJob.from_d(d)
    log.info(str(job))
    if args.json is not None:
        with open(args.json, "wt") as json_out:
            json_out.write(json.dumps(d))
            log.info("Wrote job JSON to {f}".format(f=args.json))
    if job.state in JobStates.ALL_FAILED:
        return 1
    return 0


def _get_parser():
    p = get_default_argparser_with_base_opts(
        description=__doc__,
        version=__version__,
        default_level="INFO")
    p.add_argument("job_id", help="SMRT Link Job ID (or UUID)")
    add_smrtlink_server_args(p)
    p.add_argument("--retry-on",
                   action="store",
                   type=lambda arg: [int(x) for x in arg.split(",")],
                   default=[],
                   help="HTTP error codes to retry")
    p.add_argument("--json",
                   action="store",
                   default=None,
                   help="Name of output file to write")
    p.add_argument("--max-time",
                   action="store",
                   type=int,
                   default=None,
                   help="Max time to wait before aborting")
    p.add_argument("--poll-interval",
                   action="store",
                   type=int,
                   default=60,
                   help="Time to sleep between polling for job state.  If set to zero, the program will exit immediately after getting job status, regardless of state.")
    return p


def main(argv=sys.argv):
    return pacbio_args_runner(
        argv=argv[1:],
        parser=_get_parser(),
        args_runner_func=run_args,
        alog=log,
        setup_log_func=setup_log)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
