import json
import pickle
from pyper.shell import mkdir, exists
from typing import Dict, Any

# cache of data
_cache: Dict[str, Dict[str, Any]] = {}

# load config.json
with open('config.json', 'r') as f:
	_config: Dict[str, Dict[str, Any]] = json.load(f)

class Data():
	""" Data used for modules
		saved in scratch/<section>/data.pickle
	"""
	def __init__(self, section: str):
		""" Initial setup
		
		Arguments:
			section {str} -- section name of the module
		"""
		self._section = section

		if section not in _cache:
			if exists('scratch/%s/data.pickle' % section):
				# load existing file
				self.reload()
			
			else:
				# create new file
				mkdir('scratch/%s' % section)
				_cache[section] = _config[section] if section in _config else {}
				self.update({})
	
	def __getitem__(self, key: str):
		""" Get the value of an entry.
		
		Arguments:
			key {str} -- name of the entry
		
		Returns:
			Any -- value of the entry
		"""
		return _cache[self._section][key]
	
	def __delitem__(self, key: str):
		""" Delete an entry.
		
		Arguments:
			key {str} -- name of the entry
		"""
		del _cache[self._section][key]
		self.update({})
	
	def __setitem__(self, key: str, value: Any):
		""" Set the value an entry.
		
		Arguments:
			key {str} -- name of the entry
			value {Any} -- value of the entry
		"""
		self.update({key: value})
	
	def __contains__(self, key: str):
		""" Check if self contains an entry.
		
		Arguments:
			key {str} -- name of the entry
		
		Returns:
			bool -- Whether self contains an entry
		"""
		return key in _cache[self._section]
	
	def update(self, update: Dict[str, Any]):
		""" Update entries and save to file
		
		Arguments:
			update {Dict[str, Any]} -- entries to be updated
		"""
		_cache[self._section].update(update)

		with open('scratch/%s/data.pickle' % self._section, 'wb') as f:
			pickle.dump(_cache[self._section], f)
	
	def reload(self):
		""" Read entries from file.
		"""
		with open('scratch/%s/data.pickle' % self._section, 'rb') as f:
			_cache[self._section] = pickle.load(f)