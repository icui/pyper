from pyper.main import modules
from pyper.tools.module import module
from pyper.tools.shell import echo
from pyper.pipeline import *

solver = modules['solver']
kernel = modules['kernel']

class gradient(module):
	def pipe(self):
		""" compute gradient
		"""
		# compute kernels
		kernel.run()

		# # export kernels to vtk
		# for kernel in kernel.kernels:
		# 	solver.export_kernel(kernel)