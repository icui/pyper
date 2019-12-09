from pyper.pipeline import stage, task

@stage
def testtask(a):
	print('test okay', a)