from pyper import config, shell
from pyper.core.job import Job


def header(job: Job):
    """Get job script header."""
    hh = job.walltime // 60
    mm = job.walltime - hh * 60

    return [
        '#!/bin/bash',
        '#BSUB -J %s' % (config.get('job', 'name') or f'pyper_{shell.cwd.split("/")[-1]}'),
        '#BSUB -P %s' % config.get_input('job', 'proj', 'Enter the project name of your job:'),
        '#BSUB -W %02d:%02d' % (hh, mm),
        '#BSUB -nnodes %d' % job.nnodes,
        '#BSUB -o output/lsf.%J.o',
        '#BSUB -e output/lsf.%J.e',
        '#BSUB -alloc_flags "gpumps"'
    ]


def jobexec(cmd: str):
    """Call job scheduler."""
    return 'bsub %s' % cmd


def mpiexec(cmd: str, nranks: int, _, cpus: int, gpus: int):
    """Execute mpi task."""
    nres = min(gpus, nranks) if gpus > 0 else nranks

    if nranks == 1:
        flags = '--smpiargs="off"'

    else:
        flags = ''

    return f'jsrun {flags} -n {nres} -a {nranks // nres} -c {cpus // nres} -g {gpus // nres} {cmd}'
