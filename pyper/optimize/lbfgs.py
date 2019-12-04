from pyper.tools.module import module

class lbfgs(module):
	def pipe(self):
		# compute direction
		for j in range(int(self.nsteps)):
			p.add_stage(self.add_step)
			solver.pipe()
			misfit.pipe()
			p.add_stage(self.line_search)
		
		p.add_stage(self.update_model)
	
	def line_search(self):
		pass
	
	def compute_direction(self):
		pass
	
	def update_model(self):
		pass
	
	def add_iter(self):
		pass
	
	def add_step(self):
		pass
