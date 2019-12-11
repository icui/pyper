from os import path, getcwd, fsync
from subprocess import check_call
from typing import Any

cwd = getcwd()

def _check(src: str):
	""" Make sure that tasks that require write permission are only performed in
		./scratch or ./output directory unless explicitly requested.
	
	Arguments:
		src {str} -- directory to be checked
	
	Raises:
		PermissionError: raise error when directory is not in ./scratch or ./output
	"""
	if src.startswith('scratch'): return
	if src.startswith(abspath('scratch')): return
	if src.startswith('output'): return
	if src.startswith(abspath('output')): return
	raise PermissionError('no permission to write to %s' % src)

def call(cmd: str):
	""" Call shell command
	
	Arguments:
		cmd {str} -- shell command
	
	Returns:
		int -- return value of shell command
	"""
	return check_call(cmd, shell=True)

def abspath(src: str):
	""" Get absolute path.
	
	Arguments:
		src {str} -- relative path
	
	Returns:
		str -- absolute path
	"""
	if src.startswith('/'):
		return src
	
	elif src.startswith('~'):
		return path.expanduser(src)

	else:
		return path.join(cwd, src)

def exists(src: str):
	""" Check whether a file or directory exists.
	
	Arguments:
		src {str} -- file or directory path
	
	Returns:
		str -- whether file or directory exists
	"""
	return path.exists(abspath(src))

def write(dst: str, str: str, mode: str = 'w', sudo: bool = False):
	""" Write to a text file and wait until write is complete.
	
	Arguments:
		dst {str} -- path of file to be written to
		str {str} -- content of the file
	
	Keyword Arguments:
		mode {str} -- mode to open the file to be written to (default: {'w'})
		sudo {bool} -- allow writing to files not in ./scratch or ./output (default: {False})
	"""
	if not sudo: _check(dst)
	with open(abspath(dst), mode) as f:
		f.write(str)
		f.flush()
		fsync(f.fileno())

def echo(*args: Any):
	""" Write message to ./output/output.log.
	
	Arguments:
		args {Any} -- message to be printed
	"""
	msg = ' '.join(str(arg) for arg in args) + '\n'
	write('output/output.log', msg, 'a')

def mkdir(dst: str, sudo: bool = False):
	""" Create directory (ignore existing directories).
	
	Arguments:
		dst {str} -- directory path
	
	Keyword Arguments:
		sudo {bool} -- allow create a directory that is not in ./scratch or ./output (default: {False})
	"""
	if not sudo: _check(dst)
	call('mkdir -p ' + abspath(dst))

def rm(dst: str, sudo: bool = False):
	""" Remove a file or directory.
	
	Arguments:
		dst {str} -- file or directory to be removed
	
	Keyword Arguments:
		sudo {bool} -- allow removing locations not in ./scratch or ./output (default: {False})
	"""
	if not sudo: _check(dst)
	call('rm -rf ' + abspath(dst))

def cp(src: str, dst: str, sudo: bool = False):
	""" Copy a file or directory.
	
	Arguments:
		src {str} -- file or directory to be copied
		dst {str} -- destination
	
	Keyword Arguments:
		sudo {bool} -- allow copying to locations not in ./scratch or ./output (default: {False})
	"""
	if not sudo: _check(dst)
	call('cp -r %s %s' % (abspath(src), abspath(dst)))

def mv(src: str, dst: str, sudo: bool = False):
	""" Move a file or directory.
	
	Arguments:
		src {str} -- file or directory to be moved
		dst {str} -- destination
	
	Keyword Arguments:
		sudo {bool} -- allow removing locations not in ./scratch or ./output (default: {False})
	"""
	if not sudo:
		_check(src)
		_check(dst)
	
	call('mv %s %s' % (abspath(src), abspath(dst)))

def ln(src: str, dst: str, sudo: bool = False):
	""" Create symbolic link.
	
	Arguments:
		src {str} -- file or directory to be linked
		dst {str} -- destination
	
	Keyword Arguments:
		sudo {bool} -- allow linking to locations not in ./scratch or ./output (default: {False})
	"""
	if not sudo: _check(dst)
	call('ln -s %s %s' % (abspath(src), abspath(dst)))