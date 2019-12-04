from os import path, getcwd, fsync
from subprocess import check_call

cwd = getcwd()

def call(cmd):
	""" call shell commands, raise error if return code != 0
	"""
	return check_call(cmd, shell=True)

def abspath(src):
	""" get absolute path
	"""
	return src if src.startswith('/') else path.join(cwd, src)

def exists(src):
	""" check whether a file or directory exists
	"""
	return path.exists(abspath(src))

def check_sandbox(src):
	""" make sure that tasks that require write permission are only performed in
		./scratch or ./output directory unless explicitly requested
	"""
	if src.startswith('scratch'): return
	if src.startswith(abspath('scratch')): return
	if src.startswith('output'): return
	if src.startswith(abspath('output')): return
	raise PermissionError('no permission to write to %s' % src)

def echo(*args):
	""" write message to ./output/output.log
	"""
	with open('output/output.log', 'a') as f:
		f.write(' '.join(str(msg) for msg in args) + '\n')

def write(dst, str, mode='w', sudo=False):
	""" write to a text file and wait until write is complete
	"""
	if not sudo: check_sandbox(dst)
	with open(abspath(dst), mode) as f:
		f.write(str)
		f.flush()
		fsync(f)

def mkdir(dst, sudo=False):
	""" create directory (ignore existing directories)
	"""
	if not sudo: check_sandbox(dst)
	call('mkdir -p ' + abspath(dst))

def rm(dst, sudo=False):
	""" force remove file or directory
	"""
	if not sudo: check_sandbox(dst)
	call('rm -rf ' + abspath(dst))

def cp(src, dst, sudo=False):
	""" copy file or directory
	"""
	if not sudo: check_sandbox(dst)
	call('cp -r %s %s' % (abspath(src), abspath(dst)))

def mv(src, dst, sudo=False):
	""" move file or directory
	"""
	if not sudo:
		check_sandbox(src)
		check_sandbox(dst)
	
	call('mv %s %s' % (abspath(src), abspath(dst)))

def ln(src, dst, sudo=False):
	""" create symbolic link
	"""
	if not sudo: check_sandbox(dst)
	call('ln -s %s %s' % (abspath(src), abspath(dst)))