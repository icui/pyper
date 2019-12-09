import sys
from importlib import import_module
from pyper.data import Data
from pyper.shell import cwd, mkdir, exists, abspath, echo
from typing import Dict, Any, Type

# add working directory to python path
if cwd not in sys.path:
	sys.path.append(cwd)

# all modules
_modules: Dict[str, Any] = {}

class Module(Data):
	""" Base class for pyper modules.
	
	Arguments:
		Data {pyper.data.Data} -- pyper data object
	
	Raises:
		TypeError: throws when trying to create the same module twice
	"""
	def __init__(self, section: str):
		""" Initial setup.
		
		Arguments:
			section {str} -- section of the module
		
		Raises:
			TypeError: raise an error if trying to create a module of the same section twice
		"""
		if section in _modules:
			raise TypeError('Cannot redefine module')
		
		super().__init__(section)

		if 'initialized' not in self:
			self.setup()
			self['initialized'] = True
	
	def setup(self):
		""" Optional method called before pipeline.
		"""
		pass

def load(section: str):
	""" Create or select a module object.
	
	Arguments:
		section {string} -- name of the section
	
	Returns:
		pyper.module.Module -- returns corresponding pyper module of target section
	"""
	if section not in _modules:
		name: str = Data(section)['module']
		module = import_module('pyper.modules.%s.%s' % (section, name.lower()))
		_modules[section] = getattr(module, name)(section)
	
	return _modules[section]