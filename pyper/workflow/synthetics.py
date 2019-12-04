from pyper.main import modules
from pyper.tools.module import module
from pyper.tools.shell import exists
from pyper.pipeline import *

solver = modules['solver']

class synthetics(module):
	def pipe(self):
		""" run forward simulation for each source in ./events
		"""
		
		for src in solver.events:
			if not exists('output/raw_syn/%s.raw_syn.h5' % src):
				solver.forward(src)
		
		self.move()
	
	@stage
	def move(self):
		""" move synthetics to events directory
		"""
		for src in solver.events:
			mv('output/raw_syn/%s.raw_syn.h5' % src, 'traces/%s.raw_obs.h5')