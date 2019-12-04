from pyper.system.cluster import cluster
from pyper.tools.shell import call

class slurm(cluster):
	def submit(self):
		""" create and submit job script
		"""
		# flags for job submission
		script = '#!/bin/bash\n\n' + \
			'#SBATCH --job-name=%s\n' % self.name + \
			'#SBATCH --nodes=%d\n' %  self.nnodes+ \
			'#SBATCH --gres=gpu:%d\n' % self.node_size + \
			'#SBATCH -o output/slurm.%J.o\n' + \
			'#SBATCH -e output/slurm.%J.e\n' + \
			'#SBATCH -t %s\n' % self.walltime
	
		if hasattr(self, 'mem'):
			script += '#SBATCH --mem=%s\n' % self.mem
		
		# write to job.bash
		self.write_job(script)

		# submit job
		call('sbatch scratch/system/job.bash')
	
	def mpiexec(self, cmd, nnodes, ntasks):
		""" command for running parallel task
		"""
		return 'srun -N %d -n %d %s' % (nnodes, ntasks, cmd)