import os #from here test parameters & references for doc string
import numpy as np
from pyper.module import Module
from pyper.shell import write, mkdir
from typing import List, Tuple, Union, Callable, Any

class System(Module):
	""" Base system class.
	"""
	def setup(self):
		""" Set initial parameters.
		"""
		mkdir('output')
		
		# temperoary task counter
		self._stages: List[List[int]] = [[]]

		self.update({
			# whether current process is a running task
			'running': False,
			# avoid duplicate submission
			'submitted': False,
			# number of nodes
			'nnodes': int(np.ceil(self['ntasks'] / self['node_size']))
		})
	
	def pipe(self, add_stage: bool, nprocs: int):
		""" Return a decorator that makes a function to create task.
		
		Arguments:
			add_stage {bool} -- whether the task should be executed in a new stage
			nprocs {int} -- number of processors (0 for running in head node)
		
		Returns:
			Callable -- decorator of functions
		"""
		def decorator(func: Callable):
			def piped_func(*args):
				if self['running']:
					# execute
					func(*args)
				
				else:
					# save to pipeline
					if add_stage: self.add_stage()
					self.add_task(func, args, nprocs)
					if add_stage: self.add_stage()
			
			return piped_func
		
		return decorator
	
	def task_file(self, i: int, j: int):
		""" Get the script file of j-th task of i-th stage.
		
		Arguments:
			i {int} -- i-th stage
			j {int} -- j-th task
		
		Returns:
			str -- task file name
		"""
		return 'scratch/system/task_%d.%d.py' % (i + 1, j + 1)

	def add_stage(self):
		""" Add an empty stage.
		"""
		if len(self._stages[-1]) > 0:
			self._stages.append([])

	def add_task(self, func: Callable, args: List[Any], nprocs: int):
		""" Add a task to current stage.
		
		Arguments:
			func {Callable} -- function called in the task
			args {List[Any]} -- arguments of the function
			nprocs {int} -- number of processors for the task
		"""
		self._stages[-1].append(nprocs)
		
		# get functino reference
		name: str = func.__name__
		module: str = func.__module__

		# deal with tasks added in main script
		if module == '__main__':
			print('@(main)', __file__)
			module = __file__.split('.')[0]
		
		# save task script
		task_file = self.task_file(len(self._stages) - 1, len(self._stages[-1]) - 1)
		write(task_file, 'import pyper.module\nfrom %s import %s\n%s(%s)' % (module, name, name, str(list(args))[1: -1]))
	
	def mpirun(self, tasks: Union[List[Tuple[str, str, int]], Tuple[str, str, int]]) -> int:
		""" Run mpi tasks.
		
		Arguments:
			tasks {Union[List[Tuple[str, str, int]], Tuple[str, str, int]]} -- path, command and number of processors
		
		Returns:
			int -- exit code of the system call
		
		Raises:
			NotImplementedError: abstract method defined by child class
		"""
		raise NotImplementedError
	
	def submit(self, workflow: Callable):
		""" Submit a job script.
		
		Arguments:
			workflow {Callable} -- tasks to be executed in the job script
		
		Raises:
			NotImplementedError: abstract method defined by child class
		"""
		raise NotImplementedError