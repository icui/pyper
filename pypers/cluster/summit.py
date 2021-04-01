from os import environ
from textwrap import dedent
from subprocess import check_call


# number of CPUs per node
cpus_per_node = 42

# number of GPUs per node
gpus_per_node = 6


def submit(cmd: str):
    """Write and submit job script."""
    # avoid circular import
    from pypers import basedir, getcfg, hasarg, getarg

    # hours and minutes
    walltime = getcfg('job', 'walltime')
    hh = int(walltime // 60)
    mm = int(walltime - hh * 60)

    # job script
    lines = [
        '#!/bin/bash',
        f'#BSUB -J {getcfg("job", "name")}',
        f'#BSUB -P {getcfg("job", "account")}',
        f'#BSUB -W {hh:02d}:{mm:02d}',
        f'#BSUB -nnodes {getcfg("job", "nnodes")}',
        f'#BSUB -o lsf.%J.o',
        f'#BSUB -e lsf.%J.e'
    ]

    # add main command
    lines.append(cmd + '\n')

    # write job script and submit
    basedir.writelines(lines, 'job.bash')
    check_call(f'bsub job.bash', shell=True)


def requeue():
    """Run current job again."""
    check_call('brequeue ' + environ['LSB_JOBID'], shell=True)


def mpiexec(cmd: str, nprocs: int, cpus_per_proc: int = 1, gpus_per_proc: int = 0):
    """Get the command to call MPI."""
    flags = ' --smpiargs="off"' if nprocs == 1 else ''

    return f'jsrun{flags} -n {nprocs} -a 1 -c {cpus_per_proc} -g {gpus_per_proc} {cmd}'
