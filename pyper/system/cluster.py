import pickle
import numpy as np
from sys import stderr
from time import time
from datetime import timedelta
from pyper.main import modules
from pyper.system.base import base
from pyper.tools.shell import write, call, echo, exists


class cluster(base):
	""" base class for running on job schedulers
		executed by calling system.execute() in job script
	"""
	def setup(self):
		""" get number of nodes
		"""
		super().setup()
		self.set('nnodes', int(np.ceil(self.ntasks / self.node_size)))
	
	def str_index(self, i, total):
		""" align integer output to same digits
		"""
		ndigits = len(str(total))
		istr = str(i + 1)
		return '0' * (ndigits - len(istr)) + istr
	
	def str_stage(self, stage):
		""" get stage info for output.log
		"""
		section, cmd, args = stage
		return '%s.%s(%s)' % (section, cmd, ', '.join(str(arg) for arg in args))
	
	def task_done(self, i):
		""" check whether a specific task is successfully finished
		"""
		stage = self.stages[self.stage]
		if type(stage) is list:
			stage_idx = self.str_index(self.stage, len(self.stages))
			task_file = 'scratch/system/task.%s.%s' % (stage_idx, self.str_index(i, len(stage)))
			if exists(task_file):
				with open(task_file) as f:
					lines = f.readlines()
					if len(lines) and lines[-1].startswith('task done'):
						return True
		
		return False
	
	def execute(self):
		""" main function called by job script
			execute all stages
		"""
		nstages = len(self.stages)

		# execute stages
		while self.stage < nstages:
			# record execution time
			time_start = time()

			# current stage
			stage = self.stages[self.stage]
			
			# output message
			stage_idx = self.str_index(self.stage, nstages)
			msg = '%s / %d' % (stage_idx, nstages)

			if type(stage) is list:
				# stage with multiple tasks
				echo(msg)
				stage_size = len(stage)
				tasks = []
				
				# skip finished tasks
				skipped = []

				for i in range(stage_size):
					if self.task_done(i):
						skipped.append(i)
					
					else:
						section, cmd, args, ntasks = stage[i]
						out = 'scratch/system/task.%s.%s' % (stage_idx, self.str_index(i, stage_size))
						exe = 'from pyper.main import modules; modules[\'%s\'].%s(%s)' % (section, cmd, str(args)[1: -1])
						cmd = 'python -c "%s" > %s 2>&1 && echo "task done" > %s' % (exe, out, out)
						tasks.append(('.', cmd, ntasks))
				
				# execute stage
				self.mpirun(*tasks)

				# check task execution status
				failed = False
				for i in range(stage_size):
					if i not in skipped:
						task_idx = self.str_index(i, stage_size)
						task_cmd = self.str_stage(stage[i][:3])
						if self.task_done(i):
							echo(' - task %s complete: %s' % (task_idx, task_cmd))
						else:
							echo(' * task %s failed: %s' % (task_idx, task_cmd))
							failed = True
					
				if failed: exit(-1)

				# reload module data
				for section in modules:
					modules[section].reload()
			
			else:
				# stage with single task
				echo(msg + '  ' + self.str_stage(stage))
				section, cmd, args = stage
				getattr(modules[section], cmd)(*args)

			# print execution time
			elapsed = int(round(time() - time_start))
			if elapsed > 0:
				echo(' - ' + str(timedelta(seconds=elapsed)))
			
			self.set('stage', self.stage + 1)
		
		echo('done')
	
	def write_job(self, script):
		""" write job script to scratch/system/job.bash
		"""
		# load modules
		if hasattr(self, 'modules'):
			script += '\nmodule load %s\n' % self.modules
		
		# command before execution
		if hasattr(self, 'pre_exec'):
			script += '\n%s\n' % (' && '.join(self.pre_exec) if type(self.pre_exec) is list else self.pre_exec)
		
		# execute stages
		script += '\npython -c "from pyper.main import modules; modules[\'system\'].execute()"\n'
		
		# command after execution
		if hasattr(self, 'post_exec'):
			script += '\n%s\n' % (' && '.join(self.post_exec) if type(self.post_exec) is list else self.post_exec)

		# write job script
		write('scratch/system/job.bash', script)
	
	def mpirun(self, *args):
		""" run multiple mpi tasks
		"""
		cmds = ''
		for (cwd, cmd, ntasks) in args:
			if type(ntasks) is int:
				nnodes = int(np.ceil(ntasks / self.node_size))
				cmds += 'cd %s && %s & ' % (cwd, self.mpiexec(cmd, nnodes, ntasks))
			else:
				cmds += 'cd %s && %s & ' % (cwd, cmd)
		
		return call(cmds + 'wait')
	
	def mpiexec(self, cmd, nnodes, ntasks):
		""" command for running parallel task
		"""
		raise NotImplementedError