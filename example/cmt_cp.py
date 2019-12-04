from pyper.tools.shell import cp
from pyper.main import modules

for src in pyper.solver.events:
	cp('output/raw_syn/%s.raw_syn.h5' % src, 'events/%s.h5' % src)