"""Utils for Running an external process"""

import logging
import tempfile
import shlex
import platform
import subprocess
import time
from collections import namedtuple

log = logging.getLogger(__name__)

ExtCmdResult = namedtuple("ExtCmdResult", "exit_code cmd run_time")


def run_cmd(cmd, stdout_fh, stderr_fh, shell=True, time_out=None):
    """Run external command


    :param: cmd: External command
    :param time_out: Timeout in seconds.
    :type time_out: None | int

    :return: ExtCmdResult

    This could all be bundled into a context manager

    with RunCommand('/path/stdout', '/path/to/stderr') as r:
        r.exe("echo 'exe1')
        r.exe("echo 'exe2')
        result = r.get_result() # close the file handles
    """
    # Clarify with Dave
    # add simple usecase with no file handles, get stderr back as str
    # stdout, stderr. In general, stdout can be large
    # add env={}
    # sleeptime scaling

    started_at = time.time()
    # Most of the current pacbio shell commands have aren't shlex-able
    if not shell:
        cmd = shlex.split(cmd)

    hostname = platform.node()
    log.debug("calling cmd '{c}' on {h}".format(c=cmd, h=hostname))
    process = subprocess.Popen(cmd, stderr=stderr_fh, stdout=stdout_fh, shell=shell)

    # This needs a better dynamic model
    max_sleep_time = 5
    sleep_time = 0.1
    dt = 0.1

    process.poll()
    while process.returncode is None:
        process.poll()
        time.sleep(sleep_time)
        run_time = time.time() - started_at
        if time_out is not None:
            if run_time > time_out:
                log.warn("Exceeded TIMEOUT of {t}. Killing cmd '{c}'".format(t=time_out, c=cmd))
                try:
                    # ask for forgiveness model
                    process.kill()
                except OSError:
                    # already been killed
                    pass
        if sleep_time < max_sleep_time:
            sleep_time += dt

    run_time = time.time() - started_at

    run_time = run_time
    returncode = process.returncode
    log.debug("returncode is {r} in {s:.2f} sec.".format(r=process.returncode,
                                                         s=run_time))

    return ExtCmdResult(returncode, cmd, run_time)
