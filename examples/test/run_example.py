#!/usr/bin/env python

from pyper.system import submit, stage, task
from pyper.shell import cwd

@stage
def t1(a):
	print('cwd:', cwd)
	print(a)

@stage(3)
def t2(a):
	print(a)

@task
def t3(a):
	raise('errrrr')
	print(a)

@task(4)
def t4(a):
	print(a)

def workflow():
	t1('apple')
	t2('pear')
	t2('pear')
	t3('peach')
	t4('banana')
	t2('pear')
	t2('pear')

submit(workflow)
