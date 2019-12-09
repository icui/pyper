from pyper.module import load
from pyper.modules.solver.solver import Solver
from typing import List, Optional

_module: Solver = load('solver')

def forward(src: str, save_wavefield: bool = False):
	""" Run forward simulation.
	
	Arguments:
		src {str} -- event name
	
	Keyword Arguments:
		save_wavefield {bool} -- whether to save entire forward wavefield for adjoint calculation (default: {False})
	"""
	_module.forward(src, save_wavefield)

def adjoint(src: str):
	""" Run adjoint simulation.
	
	Arguments:
		src {str} -- event name
	"""
	_module.adjoint(src)

def sum_kernels(events: List[str]):
	""" Sum kernels of different events.
	
	Arguments:
		events {List[str]} -- events to be summed
	"""
	_module.sum_kernels(events)

def export_kernel(kernel: str, src: str = 'kernels_sum'):
	""" Export kernels to a generic format.
	
	Arguments:
		kernel {str} -- kernel name to be exported
	
	Keyword Arguments:
		src {str} -- event name of the kernel (default: {'kernels_sum'})
	"""
	_module.export_kernel(kernel)

def set_duration(src: str, duration: float):
	""" Set duration of the simulation.
	
	Arguments:
		src {str} -- event name
		duration {float} -- simulation duration in seconds
	"""
	_module.set_duration(src, duration)

def set_monochronic(src: str, steady_state: Optional[float] = None):
	""" Tell solver to use a monochronic source time function.
	
	Arguments:
		src {str} -- event name
	
	Keyword Arguments:
		steady_state {Optional[float]} -- set the time for the simulation to reach steady state (default: {None})
	"""
	_module.set_monochronic(src, steady_state)