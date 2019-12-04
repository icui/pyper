from pyper.system.cluster import cluster
from pyper.tools.shell import call

class lsf(cluster):
	def submit(self):
		""" create and submit job script
		"""
		# flags for job submission
		script = '#!/bin/bash\n\n' + \
			'#BSUB -P %s\n' % self.proj + \
			'#BSUB -W %s\n' % self.walltime + \
			'#BSUB -nnodes %d\n' % self.nnodes+ \
			'#BSUB -o output/lsf.%J.o\n' + \
			'#BSUB -e output/lsf.%J.e\n' + \
			'#BSUB -J %s\n' % self.name

		# write to job.bash
		self.write_job(script)
		
		# submit job
		call('bsub scratch/system/job.bash')
	
	def mpiexec(self, cmd, nnodes, ntasks):
		""" command for running parallel task
		"""
		size = self.node_size
		return 'jsrun -n %d -a %d -c %d -g %d %s' % (nnodes, size, size, size, cmd)