from pyper.module import load
from pyper.modules.system.system import System
from typing import Union, Callable, List, Tuple, Any

_module: System = load('system')

def add_stage():
	""" Add an empty stage.
	"""
	_module.add_stage()

def add_task(func: Callable, args: List[Any], nprocs: int):
	""" Add a task to current stage.
	
	Arguments:
		func {Callable} -- function to be called in the task
		args {List[Any]} -- function arguments
		nprocs {int} -- number of processors for the task
	"""
	_module.add_task(func, args, nprocs)

def stage(nprocs: int = 0):
	""" Decorator of function which adds a task in a new stage when called
	
	Arguments:
		nprocs {int} -- number of processors (0 for running in head node)
	
	Returns:
		Callable -- Decorated function
	"""
	return _module.pipe(True, nprocs)

def task(nprocs: int = 0):
	""" Decorator of function which adds a task in current stage when called
	
	Arguments:
		nprocs {int} -- number of processors (0 for running in head node)
	
	Returns:
		Callable -- Decorated function
	"""
	return _module.pipe(False, nprocs)

def mpirun(tasks: Union[List[Tuple[str, str, int]], Tuple[str, str, int]]):
	""" Run mpi tasks.
	
	Arguments:
		tasks {Union[List[Tuple[str, str, int]], Tuple[str, str, int]]} -- path, command and number of processors
	
	Returns:
		int -- exit code of the system call
	"""
	return _module.mpirun(tasks)

def submit(workflow: Callable):
	""" Submit workflow.
	
	Arguments:
		workflow {Callable} -- tasks to be executed
	"""
	_module.submit(workflow)