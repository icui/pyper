from pyper.main import modules
from pyper.tools.module import module
from pyper.tools.shell import echo
from pyper.pipeline import *

solver = modules['solver']

class cmt(module):
	def setup(self):
		""" get events
		"""
		self.set('events', [])

	def pipe(self):
		""" compute gradient
		"""
		self.download()

		for src in solver.events:
			solver.forward(src)
	
	@stage
	def download(self):
		""" download cmtsolution files
		"""
		pass