from pyper.modules.system.cluster import Cluster

class LSF(Cluster):
	""" LSF based system.
	"""
	def _jobscript(self, script: str):
		header = [
			'#!/bin/bash\n',
			'#BSUB -P %s' % self['proj'],
			'#BSUB -W %s' % self['walltime'],
			'#BSUB -nnodes %d' % self['nnodes'],
			'#BSUB -o output/lsf.%J.o',
			'#BSUB -e output/lsf.%J.e',
			'#BSUB -J %s' % self['name']
		]
		
		return '\n'.join(header) + '\n' + script

	def _jobexec(self, src: str):
		return 'bsub %s' % src

	def _mpiexec(self, cmd: str, nnodes: int, nprocs: int):
		return 'srun -N %d -n %d %s' % (nnodes, nprocs, cmd)