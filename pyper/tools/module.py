import pickle
from pyper.tools.shell import exists, mkdir

class module:
	""" base class of modules provides tasks and stages to the pipeline
		each module is provided w/ a folder in ./scratch
		data is stored in ./scratch/<module_name>/data.pickle
		pyper is blind to the implementation of the modules except for pipeline module
	"""
	def __init__(self, config):
		""" initialize
		"""
		# read data
		section = self._section = self.__module__.split('.')[-2]
		data_file = 'scratch/' + section + '/data.pickle'
		self._data = {}

		if exists(data_file):
			# use existing data and skip self.setup()
			setup = False
			self.reload()
			
		else:
			# create new data file and initialize module
			setup = True
			mkdir('scratch/%s' % section)
			with open(data_file, 'wb') as f:
				pickle.dump(self._data, f)

		# copy config attributes
		for key in config:
			setattr(self, key, config[key])
		
		# initialize newly created module
		if setup:
			self.setup()

	def setup(self):
		""" optional initialization method, executed before pipeline
			will not be called in restored sessions
		"""
		pass
	
	def reload(self):
		""" reload data from data.pickle
		"""
		# clear current data
		for key in self._data:
			delattr(self, key)
		
		# read data
		with open('scratch/' + self._section + '/data.pickle', 'rb') as f:
			data = pickle.load(f)
		
		for key in data:
			setattr(self, key, data[key])

		self._data = data
	
	def set(self, props, value=None):
		""" update data and save to data.ini
		"""
		if type(props) is str:
			# set a single property
			self.set({props: value})

		else:
			# data and its storage path
			data = self._data
			data_file = 'scratch/' + self._section + '/data.pickle'

			# update data
			for key in props:
				if props[key] is None:
					delattr(self, key)
					del data[key]

				else:
					setattr(self, key, props[key])
					data[key] = props[key]
			
			# save data
			with open(data_file, 'wb') as f:
				pickle.dump(data, f)