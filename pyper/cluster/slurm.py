from pyper import config, shell
from pyper.core.job import Job


def header(job: Job):
    """Get job script header."""
    hh = job.walltime // 60
    mm = job.walltime - hh * 60

    lines = [
        '#!/bin/bash',
        '#SBATCH --job-name=%s' % (config.get('job', 'name') or f'pyper_{shell.cwd.split("/")[-1]}'),
        '#SBATCH -t %02d:%02d:00' % (hh, mm),
        '#SBATCH -o output/slurm.%J.o',
        '#SBATCH -e output/slurm.%J.e',
        '#SBATCH --nodes=%d' % job.nnodes
    ]

    if job.gpus_per_node:
        lines.append('#SBATCH --gres=gpu:%d' % job.gpus_per_node)

    if mem := config.get('job', 'mem'):
        lines.append('#SBATCH --mem=%s' % mem)

    return lines


def jobexec(cmd: str):
    """Call job scheduler."""
    return 'sbatch %s' % cmd


def mpiexec(cmd: str, nranks: int, nnodes: int, cpus: int, gpus: int):
    """Execute mpi task."""
    return f'srun -N {nnodes} -n {nranks} ' + \
        f'--cpus-per-task={cpus // nranks} --gpus-per-task={gpus // nranks} {cmd}'
