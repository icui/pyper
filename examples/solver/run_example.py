#!/usr/bin/env python

from pyper.solver import events, forward
from pyper.system import submit

def workflow():
	for src in events:
		forward(src)

submit(workflow)
