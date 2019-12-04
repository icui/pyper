import json
from importlib import import_module
from pyper.tools.shell import mkdir, exists
from pyper.tools.module import module
from pyper.main import modules

class base(module):
	""" base class for system
	"""

	def setup(self):
		""" initialization
		"""
		self.set('stage', 0)

	def submit(self):
		""" create and submit job script
			use existing job script if available
		"""
		raise NotImplementedError