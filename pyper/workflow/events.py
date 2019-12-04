from pyper.main import modules
from pyper.tools.module import module
from pyper.tools.shell import echo
from pyper.pipeline import *

solver = modules['solver']

class events(module):
	def pipe(self):
		""" compute gradient
		"""
		for i in range(5):
			solver.forward(solver.events[i])