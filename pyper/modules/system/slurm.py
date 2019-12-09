from pyper.modules.system.cluster import Cluster

class Slurm(Cluster):
	""" Slurm based system.
	"""
	def _jobscript(self, script: str):
		""" Create job script.
		
		Arguments:
			script {str} -- main content of job script
		
		Returns:
			str -- full job script content
		"""
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
		""" Command for job submission.
		
		Arguments:
			src {str} -- path of job script
		
		Returns:
			str -- job submission command
		"""
		return 'sbatch %s' % src

	def _mpiexec(self, cmd: str, nnodes: int, nprocs: int):
		""" Command for running tasks with multiple processors
		
		Arguments:
			cmd {str} -- command to be executed
			nnodes {int} -- number of nodes for the command
			nprocs {int} -- total number of processors for the command
		
		Returns:
			str -- task execution command
		"""
		return 'srun -N %d -n %d %s' % (nnodes, nprocs, cmd)