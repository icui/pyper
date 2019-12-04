from pyper.tools.module import module

class base(module):
	def run(self, src, mode=0):
		""" run forward or adjoint simulation
			src: name of the source
			mode = 0: forward simulation
			mode = 1: forward simulation and save forward wavefield
			mode = 2: adjoint simulation
		"""
		raise NotImplementedError
	
	def mesh(self, src):
		""" run forward simulation
		"""
		self.pipe(src, 1)

	def sum_kernels(self):
		""" sum kernels from all events
		"""
		raise NotImplementedError

	def export_kernel(self):
		""" export kernel file
		"""
		raise NotImplementedError
	
	def set_duration(self, duration):
		""" set time duration of simulation
		"""
		raise NotImplementedError
	
	def set_monochronic(self, steady_state):
		""" tell specfem to use monochronic source time function for source encoding
			and set length of steady state
		"""
		raise NotImplementedError