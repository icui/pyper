import pickle
from pyper.main import modules
from pyper.tools.shell import echo

# list of stages
_stages = []

def add_stage():
	""" add a new stage to seperate tasks
	"""
	_stages.append([])

def task(ntasks=1):
	""" create a task decorator
	"""
	def task_decorator(func):
		""" wrap a method to task
		"""
		def task_func(self, *args):
			if not hasattr(modules['system'], 'stages'):
				# create a stage containing the task
				if len(_stages) == 0 or type(_stages[-1]) is not list:
					add_stage()
				
				# save to pipeline
				_stages[-1].append((self._section, func.__name__, list(args), ntasks))
				return _stages[-1][-1]
			
			else:
				# execute
				return func(self, *args)
			
		return task_func
	
	if callable(ntasks):
		func = ntasks
		ntasks = None
		return task_decorator(func)
	
	else:
		return task_decorator


def stage(func=None):
	""" create a stage decorator
	"""
	system = modules['system']

	if func:
		# return a stage running on head node
		def stage_func(self, *args):
			if not hasattr(system, 'stages'):
				# save to pipeline
				_stages.append((self._section, func.__name__, args))
				return _stages[-1]
			
			else:
				# execute
				return func(self, *args)
		
		return stage_func
	
	else:
		# return a stage running on compute nodes
		def stage_decorator(func):
			def stage_func(self, *args):
				if not hasattr(system, 'stages'):
					# save to pipeline
					_stages.append([(self._section, func.__name__, list(args), system.ntasks)])
					return _stages[-1]
				
				else:
					# execute
					return func(self, *args)
			
			return stage_func
		
		return stage_decorator
		