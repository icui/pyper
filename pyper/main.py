import json
from types import MethodType
from importlib import import_module
from pyper.tools.shell import exists, mkdir, cp

# loaded modules
modules = {}

# create directories for storage and output
mkdir('scratch')
mkdir('output')

# read config.json to get module list
with open('config.json', 'r') as f:
	_config = json.load(f)

# load modules
def _load(section):
	name = _config[section]['module']
	module = import_module('pyper.%s.%s' % (section, name))
	modules[section] = getattr(module, name)(_config[section])

# load system module first
_load('system')

# load optional modules
for _section in _config:
	if _section != 'system' and _section != 'workflow':
		_load(_section)

# load workflow module last
_load('workflow')