from pyper.module import Module
from typing import List, Optional
from abc import abstractmethod

class Solver(Module):
	""" Base solver class.
	"""
	@abstractmethod
	def forward(self, src: str, save_wavefield: bool = False):
		""" Run forward simulation.
		
		Arguments:
			src {str} -- event name
		
		Keyword Arguments:
			save_wavefield {bool} -- whether to save entire forward wavefield for adjoint calculation (default: {False})
		"""
		pass
	
	@abstractmethod
	def adjoint(self, src: str):
		""" Run adjoint simulation.
		
		Arguments:
			src {str} -- event name
		"""
		pass
	
	@abstractmethod
	def sum_kernels(self, events: List[str]):
		""" Sum kernels of different events.
		
		Arguments:
			events {List[str]} -- events to be summed
		"""
		pass

	@abstractmethod
	def export_kernel(self, kernel: str, src: str = 'kernels_sum'):
		""" Export kernels to a generic format.
		
		Arguments:
			kernel {str} -- kernel name to be exported
		
		Keyword Arguments:
			src {str} -- event name of the kernel (default: {'kernels_sum'})
		"""
		pass

	@abstractmethod
	def set_duration(self, src: str, duration: float):
		""" Set duration of the simulation.
		
		Arguments:
			src {str} -- event name
			duration {float} -- simulation duration in seconds
		"""
		pass

	@abstractmethod
	def set_monochronic(self, src: str, steady_state: Optional[float] = None):
		""" Tell solver to use a monochronic source time function.
		
		Arguments:
			src {str} -- event name
		
		Keyword Arguments:
			steady_state {Optional[float]} -- set the time for the simulation to reach steady state (default: {None})
		"""
		pass