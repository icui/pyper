import numpy as np
from time import time
from datetime import timedelta
from pyper.modules.system.system import System
from pyper.shell import write, call, echo
from abc import abstractmethod
from typing import List, Callable, Tuple, Union

class Cluster(System):
	""" base class for cluster based systems
	"""
	@abstractmethod
	def _jobscript(self, script: str) -> str:
		""" Create job script.
		
		Arguments:
			script {str} -- main content of job script
		
		Returns:
			str -- full job script content
		"""
		pass
	
	@abstractmethod
	def _jobexec(self, src: str) -> str:
		""" Command for job submission.
		
		Arguments:
			src {str} -- path of job script
		
		Returns:
			str -- job submission command
		"""
		pass

	@abstractmethod
	def _mpiexec(self, cmd: str, nnodes: int, nprocs: int) -> str:
		""" Command for running tasks with multiple processors
		
		Arguments:
			cmd {str} -- command to be executed
			nnodes {int} -- number of nodes for the command
			nprocs {int} -- total number of processors for the command
		
		Returns:
			str -- task execution command
		"""
		pass
	
	def _execute(self):
		""" Main function called by job script,
			execute all stages.
		"""
		stages: List[List[int]] = self['stages']
		nstages = len(stages)
		idx = lambda i, n: '0' * (len(str(n)) - len(str(i + 1))) + str(i + 1)

		# execute stages
		while self['stage'] < nstages:
			# current stage
			i: int = self['stage']
			echo('%s / %d' % (idx(i, nstages), nstages))
			
			# list tasks and indices of finished tasks
			tasks: List[Tuple[str, str, int]] = []
			ntasks = len(stages[i])

			# create task list
			for j in range(ntasks):
				if '%d.%d' % (i, j) not in self['finished']:
					# task file path
					task_file = self.task_file(i, j)
					nprocs = stages[i][j]

					# output message
					with open(task_file, 'r') as f:
						line = f.readlines()[-1]

					proc = ''
					if nprocs > 0:
						proc += '%d node' % nprocs
						if nprocs > 1: proc += 's'
					
					else:
						proc = 'head node'
					
					echo(' > task %s %s (%s)' % (idx(j, ntasks), line, proc))

					# success callback
					finalize = 'python -c "from pyper import system; system._module._finalize(\'%d.%d\', \'%s\', %f)"' % \
						(i, j, idx(j, ntasks), time())

					# add task
					tasks.append(('.', 'python %s && %s' % (task_file, finalize), nprocs))
			
			# execute stage
			self['running'] = True
			self.mpirun(tasks)
			self.reload()
			self['running'] = False

			# check task execution status
			failed = False
			for j in range(ntasks):
				if '%d.%d' % (i, j) not in self['finished']:
					failed = True
					echo(' * task %s failed' % idx(j, ntasks))
				
			if failed:
				self['submitted'] = False
				exit(-1)

			self['stage'] += 1
		
		echo('done')

	def _finalize(self, ij: str, idx: str, time_start: float):
		""" Callback when task ended successfully.
		
		Arguments:
			ij {str} -- id of task
			idx {str} -- index of task in current stage
			time_start {float} -- task start time
		"""
		# mark as finished
		finished: List[str] = self['finished']
		finished.append(ij)
		self['finished'] = finished

		# print output
		elapsed = str(timedelta(seconds=int(round(time() - time_start))))
		echo(' - task %s complete (%s)' % (idx, elapsed))
	
	def mpirun(self, tasks: Union[List[Tuple[str, str, int]], Tuple[str, str, int]]):
		cmds = ''
		if type(tasks) is tuple:
			tasks = [tasks]
		
		for (cwd, cmd, nprocs) in tasks:
			if nprocs > 0:
				nnodes = int(np.ceil(nprocs / self['node_size']))
				cmds += 'cd %s && %s & ' % (cwd, self._mpiexec(cmd, nnodes, nprocs))
			
			else:
				cmds += 'cd %s && %s & ' % (cwd, cmd)
		
		return call(cmds + 'wait')

	def submit(self, workflow: Callable):
		# avoid duplicate submission
		if self['submitted']:
			return
		
		# create workflow
		if hasattr(self, '_stages'):
			workflow()

			if len(self._stages[-1]) == 0:
				self._stages.pop()

			self.update({'submitted': True, 'stage': 0, 'stages': self._stages, 'finished': []})

			# job script header
			script = ''

			# command before workflow execution
			if 'pre_exec' in self:
				script += '\n%s\n' % '\n'.join(self['pre_exec'])
			
			script += '\npython -c "from pyper import system; system._module._execute()"\n'

			# command before workflow execution
			if 'post_exec' in self:
				script += '\n%s\n' % '\n'.join(self['post_exec'])

			# write job script
			write('scratch/system/job.bash', self._jobscript(script))
		
		else:
			echo('------------')
			self['submitted'] = True

		# submit job script
		call(self._jobexec('scratch/system/job.bash'))