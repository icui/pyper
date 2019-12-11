from pyper.modules.system.cluster import Cluster

class Slurm(Cluster):
	""" Slurm based system.
	"""
	def _jobscript(self, script: str):
		header = [
			'#!/bin/bash',
			'#SBATCH --job-name=%s' % self['name'],
			'#SBATCH --nodes=%d' % self['nnodes'],
			'#SBATCH --gres=gpu:%d' % self['node_size'],
			'#SBATCH -o output/slurm.%J.o',
			'#SBATCH -e output/slurm.%J.e',
			'#SBATCH -t %s' % self['walltime']
		]

		if 'mem' in self:
			header.append('#SBATCH --mem=%s' % self['mem'])
		
		return '\n'.join(header) + '\n' + script

	def _jobexec(self, src: str):
		return 'sbatch %s' % src

	def _mpiexec(self, cmd: str, nnodes: int, nprocs: int):
		return 'srun -N %d -n %d %s' % (nnodes, nprocs, cmd)