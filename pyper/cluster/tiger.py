from pyper.cluster.slurm import header, jobexec, mpiexec

# number of CPUs per node
cpus_per_node = 4

# number of gpus per node
gpus_per_node = 4

__all__ = ['header', 'jobexec', 'mpiexec', 'cpus_per_node', 'gpus_per_node']
